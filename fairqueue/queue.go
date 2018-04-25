package fairqueue

import (
	"bytes"
	"encoding/binary"
	"math/rand"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/subspace"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
)

type QueueWork struct {
	Priority  int64
	Tenant    string
	Partition string
	Data      interface{}
}

type FairQueueLayer struct {
	root           subspace.Subspace
	data           subspace.Subspace
	tenants        subspace.Subspace
	partitions     subspace.Subspace
	partitionLocks subspace.Subspace
	offsets        subspace.Subspace
}

func NewFairQueueLayer(space subspace.Subspace) *FairQueueLayer {
	return &FairQueueLayer{
		root:           space,
		data:           space.Sub("data"),
		tenants:        space.Sub("tenants"),
		partitions:     space.Sub("partitions"),
		partitionLocks: space.Sub("partitions-locks"),
		offsets:        space.Sub("offsets"),
	}
}

func (layer *FairQueueLayer) Push(
	tx fdb.Transaction,
	work *QueueWork,
) error {
	if err := layer.writeData(tx, work); err != nil {
		return err
	}

	if err := layer.incrementTenantCount(tx, work); err != nil {
		return err
	}

	if err := layer.incrementPartitionCount(tx, work); err != nil {
		return err
	}

	return nil
}

func (layer *FairQueueLayer) Pop(
	tx fdb.Transaction,
	clientID string,
) (*QueueWork, error) {
	offset, err := layer.getClientOffset(tx, clientID)
	if err != nil {
		return nil, err
	}

	queueInfo, err := layer.getNextQueueFromOffset(tx, offset)
	if err == nil && queueInfo == nil {
		queueInfo, err = layer.getNextQueueFromOffset(tx, nil)
	}
	if err != nil || queueInfo == nil {
		return nil, err
	}

	partitionInfo, err := layer.getNextPartitionFromQueue(tx, queueInfo)
	if err == nil && partitionInfo == nil {
		partitionInfo, err = layer.getNextPartitionFromQueue(tx, nil)
	}
	if err != nil || partitionInfo == nil {
		return nil, err
	}

	layer.lockPartition(tx, partitionInfo)
	layer.commitOffset(tx, clientID, partitionInfo)

	if err := layer.decrementPartitionCount(tx, partitionInfo); err != nil {
		return nil, err
	}
	if err := layer.decrementTenantCount(tx, partitionInfo); err != nil {
		return nil, err
	}

	work, err := layer.getData(tx, partitionInfo)
	if err != nil {
		return nil, err
	}

	return work, nil
}

func (layer *FairQueueLayer) Commit(
	tx fdb.Transaction,
	work *QueueWork,
) error {
	if err := layer.deleteWork(tx, work); err != nil {
		return err
	}

	partitionInfo := &partitionInfo{
		priority:  work.Priority,
		tenant:    work.Tenant,
		partition: work.Partition,
	}

	layer.unlockPartition(tx, partitionInfo)

	if err := layer.decrementPartitionLockCount(tx, partitionInfo); err != nil {
		return err
	}

	return nil
}

func (layer *FairQueueLayer) decrementPartitionLockCount(
	tx fdb.Transaction,
	partition *partitionInfo,
) error {
	lockBaseKey := layer.partitionLocks.Sub(partition.tenant, partition.partition)
	lockCountKey := lockBaseKey.Sub("count")
	if err := decrementKey(tx, lockCountKey); err != nil {
		return err
	}
	cleared, err := clearKeyIfZero(tx, lockCountKey)
	if cleared && err == nil {
		tx.Clear(lockBaseKey.Sub("locked"))
	}
	return err
}

func (layer *FairQueueLayer) writeData(
	tx fdb.Transaction,
	work *QueueWork,
) error {
	partitionKey := layer.data.Sub(work.Tenant, work.Partition)
	nextIndex, err := getLastIndex(tx, partitionKey)
	if err != nil {
		return err
	}
	key := partitionKey.Sub(nextIndex, rand.Int63())
	value := tuple.Tuple{work.Data}
	tx.Set(key, value.Pack())
	return nil
}

func (layer *FairQueueLayer) decrementTenantCount(
	tx fdb.Transaction,
	partition *partitionInfo,
) error {
	baseKey := layer.tenants.Sub(partition.priority, partition.tenant, "count")
	if err := decrementKey(tx, baseKey); err != nil {
		return err
	}
	_, err := clearKeyIfZero(tx, baseKey)
	return err
}

func (layer *FairQueueLayer) decrementPartitionCount(
	tx fdb.Transaction,
	partition *partitionInfo,
) error {
	baseKey := layer.partitions.Sub(partition.priority, partition.tenant, partition.partition, "count")
	if err := decrementKey(tx, baseKey); err != nil {
		return err
	}
	_, err := clearKeyIfZero(tx, baseKey)
	return err
}

func (layer *FairQueueLayer) incrementTenantCount(
	tx fdb.Transaction,
	work *QueueWork,
) error {
	baseKey := layer.tenants.Sub(work.Priority, work.Tenant, "count")
	return incrementKey(tx, baseKey)
}

func (layer *FairQueueLayer) incrementPartitionCount(
	tx fdb.Transaction,
	work *QueueWork,
) error {
	baseKey := layer.partitions.Sub(work.Priority, work.Tenant, work.Partition)

	lockBaseKey := layer.partitionLocks.Sub(work.Tenant, work.Partition)
	lockedKey := lockBaseKey.Sub("locked")
	valueBytes, err := tx.Get(lockedKey).Get()
	if err != nil {
		return err
	}
	if len(valueBytes) == 0 {
		tx.Set(lockedKey, tuple.Tuple{false}.Pack())
	}
	if err := incrementKey(tx, lockBaseKey.Sub("count")); err != nil {
		return err
	}
	return incrementKey(tx, baseKey.Sub("count"))
}

func addInt64(tx fdb.Transaction, key fdb.KeyConvertible, v int64) error {
	var bytes bytes.Buffer
	err := binary.Write(&bytes, binary.LittleEndian, v)
	if err != nil {
		return err
	}
	tx.Add(key, bytes.Bytes())
	return nil
}

func incrementKey(tx fdb.Transaction, key fdb.KeyConvertible) error {
	return addInt64(tx, key, 1)
}

func decrementKey(tx fdb.Transaction, key fdb.KeyConvertible) error {
	return addInt64(tx, key, -1)
}

func clearKeyIfZero(tx fdb.Transaction, key fdb.KeyConvertible) (bool, error) {
	valueBytes, err := tx.Get(key).Get()
	if err != nil {
		return false, err
	}

	bytes := bytes.NewBuffer(valueBytes)
	v := int64(0)
	err = binary.Read(bytes, binary.LittleEndian, &v)
	if err != nil {
		return false, err
	}

	if v == 0 {
		tx.Clear(key)
		return true, nil
	}

	return false, nil
}

type clientOffset struct {
	priority *int64
	tenant   *string
}

func (layer *FairQueueLayer) clientOffsetKey(clientID string) fdb.Key {
	return layer.offsets.Pack(tuple.Tuple{clientID})
}

func (layer *FairQueueLayer) getClientOffset(
	tx fdb.Transaction,
	clientID string,
) (*clientOffset, error) {
	offsetBytes, err := tx.Get(layer.clientOffsetKey(clientID)).Get()
	if err != nil {
		return nil, err
	}

	offset, err := tuple.Unpack(offsetBytes)
	if err != nil || len(offset) == 0 {
		return nil, err
	}

	priority := offset[0].(int64)
	tenant := offset[1].(string)

	return &clientOffset{
		priority: &priority,
		tenant:   &tenant,
	}, nil
}

type partitionInfo struct {
	priority  int64
	tenant    string
	partition string
}

func (layer *FairQueueLayer) rangeFromOffset(
	tx fdb.Transaction,
	offset *clientOffset,
) (fdb.Range, error) {
	space := layer.partitions
	if offset == nil {
		return space, nil
	}

	tuple := tuple.Tuple{}
	if offset.priority != nil {
		tuple = append(tuple, *offset.priority)
		if offset.tenant != nil {
			tuple = append(tuple, *offset.tenant)
		}
	}
	searchSpace := space
	if len(tuple) > 1 {
		spaceTuple := tuple[0 : len(tuple)-2]
		searchSpace = space.Sub(spaceTuple...)
	}
	_, end := searchSpace.FDBRangeKeySelectors()
	return fdb.SelectorRange{
		Begin: fdb.FirstGreaterThan(space.Pack(tuple)),
		End:   end,
	}, nil
}

func (layer *FairQueueLayer) getNextQueueFromOffset(
	tx fdb.Transaction,
	offset *clientOffset,
) (*clientOffset, error) {
	var offsetRange fdb.Range
	offsetRange = layer.tenants
	if offset != nil {
		key := layer.tenants.Pack(tuple.Tuple{*offset.priority, *offset.tenant, "count"})
		_, end := layer.tenants.Sub(*offset.priority).FDBRangeKeySelectors()
		offsetRange = fdb.SelectorRange{
			Begin: fdb.FirstGreaterThan(key),
			End:   end,
		}
	}
	results, err := tx.Snapshot().GetRange(offsetRange, fdb.RangeOptions{1, -1, false}).GetSliceWithError()
	if err != nil || len(results) == 0 {
		return nil, err
	}

	key, err := layer.tenants.Unpack(results[0].Key)
	if err != nil {
		return nil, err
	}

	priority := key[0].(int64)
	tenant := key[1].(string)
	return &clientOffset{
		priority: &priority,
		tenant:   &tenant,
	}, nil
}

func (layer *FairQueueLayer) getNextPartitionFromQueue(
	tx fdb.Transaction,
	offset *clientOffset,
) (*partitionInfo, error) {
	partitionRange, err := layer.rangeFromOffset(tx, offset)
	if err != nil {
		return nil, err
	}
	partitionIterator := tx.Snapshot().GetRange(partitionRange, fdb.RangeOptions{}).Iterator()
	for partitionIterator.Advance() {
		kv, err := partitionIterator.Get()
		if err != nil {
			return nil, err
		}
		key, err := layer.partitions.Unpack(kv.Key)
		if err != nil {
			return nil, err
		}
		priority := key[0].(int64)
		tenant := key[1].(string)
		partition := key[2].(string)

		lockedKey := layer.partitionLocks.Sub(tenant, partition, "locked")
		valueBytes, err := tx.Get(lockedKey).Get()
		if err != nil {
			return nil, err
		}
		value, err := tuple.Unpack(valueBytes)
		if err != nil {
			return nil, err
		}
		if !value[0].(bool) {
			return &partitionInfo{
				priority:  priority,
				tenant:    tenant,
				partition: partition,
			}, nil
		}
	}
	return nil, nil
}

func (layer *FairQueueLayer) setLockState(
	tx fdb.Transaction,
	queue *partitionInfo,
	locked bool,
) {
	key := layer.partitionLocks.Sub(
		queue.tenant,
		queue.partition,
		"locked",
	)
	tx.Set(key, tuple.Tuple{locked}.Pack())
}

func (layer *FairQueueLayer) lockPartition(
	tx fdb.Transaction,
	queue *partitionInfo,
) {
	layer.setLockState(tx, queue, true)
}

func (layer *FairQueueLayer) unlockPartition(
	tx fdb.Transaction,
	queue *partitionInfo,
) {
	layer.setLockState(tx, queue, false)
}

func (layer *FairQueueLayer) commitOffset(
	tx fdb.Transaction,
	clientID string,
	queue *partitionInfo,
) {
	offset := tuple.Tuple{queue.priority, queue.tenant}
	tx.Set(layer.clientOffsetKey(clientID), offset.Pack())
}

func (layer *FairQueueLayer) getData(
	tx fdb.Transaction,
	queue *partitionInfo,
) (*QueueWork, error) {
	partitionKey := layer.data.Sub(queue.tenant, queue.partition)
	values, err := tx.GetRange(partitionKey, fdb.RangeOptions{1, -1, false}).GetSliceWithError()
	if err != nil || len(values) == 0 {
		return nil, err
	}
	value, err := tuple.Unpack(values[0].Value)
	if err != nil {
		return nil, err
	}
	return &QueueWork{
		Priority:  queue.priority,
		Tenant:    queue.tenant,
		Partition: queue.partition,
		Data:      value[0],
	}, nil
}

func (layer *FairQueueLayer) deleteWork(
	tx fdb.Transaction,
	work *QueueWork,
) error {
	partitionKey := layer.data.Sub(work.Tenant, work.Partition)
	values, err := tx.GetRange(partitionKey, fdb.RangeOptions{1, -1, false}).GetSliceWithError()
	if err != nil || len(values) == 0 {
		return err
	}
	tx.Clear(values[0].Key)
	return nil
}

func getLastIndex(tx fdb.Transaction, s subspace.Subspace) (int64, error) {
	keySnapshot, err := tx.Snapshot().GetRange(s, fdb.RangeOptions{1, -1, true}).GetSliceWithError()
	if err != nil {
		return 0, err
	}

	if len(keySnapshot) == 0 {
		return 0, nil
	}
	key, err := s.Unpack(keySnapshot[0].Key)
	return key[0].(int64) + 1, err
}

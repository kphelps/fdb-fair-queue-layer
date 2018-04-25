package fairqueue_test

import (
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
	. "github.com/kphelps/fdb-fair-queue-layer/fairqueue"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("FairQueueLayer", func() {

	var db fdb.Database
	var dir directory.DirectorySubspace
	var layer *FairQueueLayer

	client1 := "client1"

	BeforeEach(func() {
		fdb.MustAPIVersion(200)
		newDB, err := fdb.OpenDefault()
		Expect(err).NotTo(HaveOccurred())
		db = newDB
		dir, err = directory.CreateOrOpen(db, []string{"test"}, nil)
		Expect(err).NotTo(HaveOccurred())
		_, err = db.Transact(func(tx fdb.Transaction) (interface{}, error) {
			tx.ClearRange(fdb.KeyRange{fdb.Key(""), fdb.Key{0xFF}})
			return nil, nil
		})
		Expect(err).NotTo(HaveOccurred())
		layer = NewFairQueueLayer(dir.Sub())
	})

	AfterEach(func() {
		_, err := db.Transact(func(tx fdb.Transaction) (interface{}, error) {
			// tx.ClearRange(fdb.KeyRange{fdb.Key(""), fdb.Key{0xFF}})
			return nil, nil
		})
		Expect(err).NotTo(HaveOccurred())
	})

	Describe("Push/Pop", func() {

		It("Works", func() {
			data := QueueWork{
				Priority:  1,
				Tenant:    "A",
				Partition: "x",
			}
			_, err := db.Transact(func(tx fdb.Transaction) (interface{}, error) {
				return nil, layer.Push(tx, &data)
			})
			Expect(err).NotTo(HaveOccurred())
			out, err := db.Transact(func(tx fdb.Transaction) (interface{}, error) {
				return layer.Pop(tx, client1)
			})
			fmt.Println(out)
			Expect(err).NotTo(HaveOccurred())
			Expect(out.(*QueueWork)).To(Equal(&data))
		})

		It("Returns nil when all are locked", func() {
			data := QueueWork{
				Priority:  1,
				Tenant:    "A",
				Partition: "x",
			}
			out, err := db.Transact(func(tx fdb.Transaction) (interface{}, error) {
				if err := layer.Push(tx, &data); err != nil {
					return nil, err
				}
				if _, err := layer.Pop(tx, client1); err != nil {
					return nil, err
				}
				return layer.Pop(tx, client1)
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(out.(*QueueWork)).To(BeNil())
		})

		It("Returns work after unlocking partition", func() {
			data := QueueWork{
				Priority:  1,
				Tenant:    "A",
				Partition: "x",
			}
			_, err := db.Transact(func(tx fdb.Transaction) (interface{}, error) {
				return nil, layer.Push(tx, &data)
			})

			Expect(err).NotTo(HaveOccurred())
			_, err = db.Transact(func(tx fdb.Transaction) (interface{}, error) {
				return nil, layer.Push(tx, &data)
			})
			Expect(err).NotTo(HaveOccurred())

			out, err := db.Transact(func(tx fdb.Transaction) (interface{}, error) {
				return layer.Pop(tx, client1)
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(out).To(Equal(&data))

			out, err = db.Transact(func(tx fdb.Transaction) (interface{}, error) {
				return layer.Pop(tx, client1)
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(out).To(BeNil())

			_, err = db.Transact(func(tx fdb.Transaction) (interface{}, error) {
				return nil, layer.Commit(tx, &data)
			})
			Expect(err).NotTo(HaveOccurred())

			out, err = db.Transact(func(tx fdb.Transaction) (interface{}, error) {
				return layer.Pop(tx, client1)
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(out).To(Equal(&data))
		})

		Context("Many queue items", func() {
			item1 := QueueWork{1, "A", "x", "1"}
			item2 := QueueWork{1, "A", "x", "2"}
			item3 := QueueWork{1, "A", "y", "3"}
			item4 := QueueWork{2, "A", "x", "4"}
			item5 := QueueWork{1, "B", "x", "5"}
			item6 := QueueWork{2, "B", "a", "6"}
			item7 := QueueWork{3, "C", "z", "7"}
			item8 := QueueWork{1, "A", "z", "8"}
			item9 := QueueWork{1, "B", "z", "9"}

			inputOrder := []QueueWork{
				item1,
				item2,
				item3,
				item4,
				item5,
				item6,
				item7,
				item8,
				item9,
			}

			expectedOrder := []QueueWork{
				item1,
				item5,
				item3,
				item9,
				item8,
				item6,
				item7,
			}

			It("Works", func() {
				for _, input := range inputOrder {
					_, err := db.Transact(func(tx fdb.Transaction) (interface{}, error) {
						return nil, layer.Push(tx, &input)
					})
					Expect(err).NotTo(HaveOccurred())
				}

				out := []QueueWork{}
				for {
					value, err := db.Transact(func(tx fdb.Transaction) (interface{}, error) {
						return layer.Pop(tx, client1)
					})
					Expect(err).NotTo(HaveOccurred())
					if value.(*QueueWork) == nil {
						break
					}
					out = append(out, *value.(*QueueWork))
				}
				Expect(out).To(Equal(expectedOrder))
			})

			Measure("Performance", func(b Benchmarker) {
				producers := 300
				consumers := 100
				tenants := 100
				priorities := 10
				partitions := 100
				messagesPerProducer := 10
				messages := messagesPerProducer * producers
				consumedMessages := uint64(0)
				var wg sync.WaitGroup
				wg.Add(producers)
				for i := 0; i < producers; i++ {
					go func() {
						for i := 0; i < messagesPerProducer; i++ {
							message := generateData(tenants, priorities, partitions)
							_, err := db.Transact(func(tx fdb.Transaction) (interface{}, error) {
								return nil, layer.Push(tx, message)
							})
							Expect(err).NotTo(HaveOccurred())
						}
						wg.Done()
					}()
				}
				b.Time("publish", func() {
					wg.Wait()
				})
				wg.Add(consumers)
				for i := 0; i < consumers; i++ {
					clientID := fmt.Sprintf("client-%d", i)
					go func() {
						for {
							count := atomic.LoadUint64(&consumedMessages)
							fmt.Println(count)
							if count == uint64(messages) {
								break
							}
							message, err := db.Transact(func(tx fdb.Transaction) (interface{}, error) {
								return layer.Pop(tx, clientID)
							})
							Expect(err).NotTo(HaveOccurred())
							if message.(*QueueWork) == nil {
								time.Sleep(100 * time.Millisecond)
								continue
							} else {
								atomic.AddUint64(&consumedMessages, 1)
							}
							_, err = db.Transact(func(tx fdb.Transaction) (interface{}, error) {
								return nil, layer.Commit(tx, message.(*QueueWork))
							})
							Expect(err).NotTo(HaveOccurred())
						}
						wg.Done()
					}()
				}
				b.Time("consume", func() {
					wg.Wait()
				})
				message, err := db.Transact(func(tx fdb.Transaction) (interface{}, error) {
					return layer.Pop(tx, "end")
				})
				Expect(err).NotTo(HaveOccurred())
				Expect(message.(*QueueWork)).To(BeNil())
			}, 1)
		})
	})
})

func generateData(
	tenants int,
	priorities int,
	partitions int,
) *QueueWork {
	tenant := fmt.Sprintf("tenant-%d", rand.Intn(tenants))
	priority := rand.Intn(priorities)
	partition := fmt.Sprintf("partition-%d", rand.Intn(partitions))
	return &QueueWork{
		Priority:  int64(priority),
		Tenant:    tenant,
		Partition: partition,
		Data:      "hello",
	}
}

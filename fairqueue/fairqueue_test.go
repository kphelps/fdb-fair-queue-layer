package fairqueue_test

import (
	"fmt"
	"math/rand"
	"sync"

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
		layer = NewFairQueueLayer(dir.Sub())
	})

	AfterEach(func() {
		db.Transact(func(tx fdb.Transaction) (interface{}, error) {
			tx.Clear(dir)
			dir.Remove(tx, []string{})
			return nil, nil
		})
	})

	Describe("Push/Pop", func() {

		data := QueueWork{
			Priority:  1,
			Tenant:    "A",
			Partition: "x",
		}

		It("Works", func() {
			out, err := db.Transact(func(tx fdb.Transaction) (interface{}, error) {
				if err := layer.Push(tx, &data); err != nil {
					return nil, err
				}
				return layer.Pop(tx, client1)
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(out.(*QueueWork)).To(Equal(&data))
		})

		It("Returns nil when empty", func() {
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
				for _, data := range inputOrder {
					_, err := db.Transact(func(tx fdb.Transaction) (interface{}, error) {
						return nil, layer.Push(tx, &data)
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
				producers := 10
				consumers := 1
				tenants := 5
				priorities := 3
				partitions := 100
				messages := 1000
				var wg sync.WaitGroup
				wg.Add(producers)
				wg.Add(consumers)
				for i := 0; i < producers; i++ {
					go func() {
						messageCount := messages / producers
						messages := make([]*QueueWork, messageCount)
						for n := 0; n < messageCount; n++ {
							messages[n] = generateData(tenants, priorities, partitions)
						}
						for _, message := range messages {
							_, _ = db.Transact(func(tx fdb.Transaction) (interface{}, error) {
								return nil, layer.Push(tx, message)
							})
						}
						wg.Done()
					}()
				}
				for i := 0; i < consumers; i++ {
					go func() {
						clientID := fmt.Sprintf("client-%d", i)
						for {
							message, _ := db.Transact(func(tx fdb.Transaction) (interface{}, error) {
								return layer.Pop(tx, clientID)
							})
							if message.(*QueueWork) == nil {
								break
							}
						}
						wg.Done()
					}()
				}
				b.Time("process", func() {
					wg.Wait()
				})
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

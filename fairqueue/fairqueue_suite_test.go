package fairqueue_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"testing"
)

func TestFairqueue(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Fairqueue Suite")
}

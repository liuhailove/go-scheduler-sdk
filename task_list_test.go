package gs

import (
	"fmt"
	"sync"
	"testing"
)

func TestEmpty(t *testing.T) {
	taskLl := &taskList{
		data: sync.Map{},
	}
	fmt.Println(taskLl.Empty())
	taskLl.Set("a", nil)
	fmt.Println(taskLl.Empty())

}

func TestReturnGeneral(t *testing.T) {
	fmt.Println(string(returnGeneral()))
}

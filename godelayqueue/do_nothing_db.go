package godelayqueue

type taskDoNothingDb struct{}

func (td *taskDoNothingDb) Save(task *Task) error {
	return nil
}

func (td *taskDoNothingDb) GetList() []*Task {
	return []*Task{}
}

func (td *taskDoNothingDb) Delete(taskId string) error {
	return nil
}

func (td *taskDoNothingDb) RemoveAll() error {
	return nil
}

func (td *taskDoNothingDb) GetWheelTimePointer() int {
	return 0
}

func (td *taskDoNothingDb) SaveWheelTimePointer(index int) error {
	return nil
}

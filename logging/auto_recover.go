package logging

import (
	"github.com/pkg/errors"
)

func RunWithRecover(f func(), logger Logger) {
	defer func() {
		if err := recover(); err != nil {
			logger.Error("Unexpected panic in util.RunWithRecover()", "err", errors.Errorf("%+v", err))
		}
	}()
	f()
}

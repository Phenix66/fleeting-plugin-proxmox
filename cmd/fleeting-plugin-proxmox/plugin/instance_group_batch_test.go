package plugin

import (
	"errors"
	"testing"

	hclog "github.com/hashicorp/go-hclog"
	"github.com/stretchr/testify/require"
)

func TestBatchError(t *testing.T) {
	errFirst := errors.New("first failure")
	errSecond := errors.New("second failure")

	testCases := []struct {
		name    string
		errs    []error
		wantErr bool
	}{
		{
			name: "all succeed",
			errs: []error{nil, nil, nil},
		},
		{
			name: "partial success is not an error",
			errs: []error{nil, errFirst, nil, errSecond, nil},
		},
		{
			name:    "all attempted failed",
			errs:    []error{errFirst, errSecond},
			wantErr: true,
		},
		{
			name: "nothing attempted",
		},
	}

	ig := &InstanceGroup{log: hclog.NewNullLogger()}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			err := ig.batchError("batch failed", testCase.errs)

			if !testCase.wantErr {
				require.NoError(t, err)

				return
			}

			// Every individual failure must survive the aggregate.
			for _, expectedErr := range testCase.errs {
				require.ErrorIs(t, err, expectedErr)
			}
		})
	}
}

func TestRunParallel(t *testing.T) {
	errFirst := errors.New("first failure")

	errs := runParallel(3, func(index int) error {
		if index == 1 {
			return errFirst
		}

		return nil
	})

	// Every call's error lands in its own slot, so a failure stays matched to its index.
	require.Equal(t, []error{nil, errFirst, nil}, errs)

	require.Empty(t, runParallel(0, func(int) error { return errFirst }))

	// A panic in one call becomes that slot's error instead of taking the process down, and
	// the other slots still complete.
	panicked := runParallel(3, func(index int) error {
		if index == 2 {
			panic("boom")
		}

		return nil
	})

	require.ErrorIs(t, panicked[2], ErrInstanceOperationPanic)
	require.ErrorContains(t, panicked[2], "boom")
	require.NoError(t, panicked[0])
	require.NoError(t, panicked[1])
}

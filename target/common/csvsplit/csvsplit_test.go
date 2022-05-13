package csvsplit

import (
	"bytes"
	"context"
	"testing"

	"github.com/samjbobb/mammoth/utils"
	"github.com/stretchr/testify/assert"
	"go.uber.org/goleak"
)

const csvFromPostgres = `one,two,three
1,value value,value value
2,value' value,value value
3,"""value"" value",value value
4,value value,"value
value"
5,"value
value",value value
6,value	value,value value
7,value	value,value value
`

var expected = []string{
	`one,two,three
1,value value,value value
2,value' value,value value
3,"""value"" value",value value
`,
	`one,two,three
4,value value,"value
value"
5,"value
value",value value
6,value	value,value value
`,
	`one,two,three
7,value	value,value value
`,
}

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func TestSplit(t *testing.T) {
	source := bytes.NewBufferString(csvFromPostgres)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	chunks, splitErrs, err := Split(ctx, 3, true, source, &PassThrough{})
	assert.NoError(t, err)

	sinkErrs := make(chan error, 1)
	var got []string
	go func() {
		defer close(sinkErrs)
		for chunk := range chunks {
			var buf bytes.Buffer
			if _, err := buf.ReadFrom(chunk); err != nil {
				sinkErrs <- err
				return
			}
			got = append(got, buf.String())
		}
	}()

	err = utils.WaitForPipeline(ctx, splitErrs, sinkErrs)
	assert.Equal(t, expected, got)
	assert.NoError(t, err)
}

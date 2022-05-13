package arraytojson

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPGArrayToJSON(t *testing.T) {
	type args struct {
		src string
	}
	tests := []struct {
		name    string
		args    args
		want    string
		wantErr bool
	}{
		{
			name:    "one dimensional array",
			args:    args{src: "{foo,bar,bim}"},
			want:    `["foo","bar","bim"]`,
			wantErr: false,
		},
		{
			name:    "multi dimensional array",
			args:    args{src: "{{meeting,lunch},{training,presentation}}"},
			want:    `[["meeting","lunch"],["training","presentation"]]`,
			wantErr: false,
		},
		{
			name:    "empty array",
			args:    args{src: "{}"},
			want:    `[]`,
			wantErr: false,
		},
		{
			name:    "null",
			args:    args{src: ""},
			want:    ``,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := PGArrayToJSON(tt.args.src)
			assert.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

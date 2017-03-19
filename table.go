package esu

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/fatih/color"
)

var (
	DefaultFirstColumnColor = color.New(color.FgYellow)
	DefaultHeaderColor      = color.New(color.FgGreen, color.Underline)
	DefaultPadding          = 2

	ansi = regexp.MustCompile("[\u001b\u009b][[()#;?]*(?:[0-9]{1,4}(?:;[0-9]{0,4})*)?[0-9A-ORZcf-nqry=><]")
)

type Table struct {
	FirstColumnColor *color.Color
	HeaderColor      *color.Color
	Padding          int

	header []string
	rows   [][]string
	widths []int
}

func NewTable(cols ...string) *Table {
	t := Table{
		HeaderColor:      DefaultHeaderColor,
		FirstColumnColor: DefaultFirstColumnColor,
		Padding:          DefaultPadding,

		header: cols,
		widths: make([]int, len(cols)),
	}

	for i, col := range cols {
		t.widths[i] = len(col) + t.Padding
	}

	return &t
}

func (t *Table) Add(vals ...interface{}) {
	row := make([]string, len(t.header))
	for i, val := range vals {
		if i >= len(t.header) {
			break
		}

		row[i] = fmt.Sprint(val)
		l := len(row[i])

		if l+t.Padding > t.widths[i] {
			t.widths[i] = l + t.Padding
		}
	}
	t.rows = append(t.rows, row)
}

func (t *Table) Print() {
	fmt.Println()
	rowFmt := t.rowFmt()
	t.printHeader(rowFmt)

	for _, row := range t.rows {
		t.printRow(rowFmt, row)
	}
}

func (t *Table) printHeader(rowFmt string) {
	row := applyWidths(t.header, t.widths)
	if t.HeaderColor != nil {
		txt := t.HeaderColor.SprintfFunc()(rowFmt, stringToInterface(row)...)
		fmt.Print(txt)
	} else {
		fmt.Printf(rowFmt, stringToInterface(row)...)
	}
}

func (t *Table) printRow(rowFmt string, row []string) {
	row = applyWidths(row, t.widths)

	if t.FirstColumnColor != nil {
		row[0] = t.FirstColumnColor.SprintFunc()(row[0])
	}

	fmt.Printf(rowFmt, stringToInterface(row)...)
}

func (t *Table) rowFmt() string {
	return strings.Repeat("%s", len(t.header)) + "\n"
}

func intToInterface(a []int) []interface{} {
	out := make([]interface{}, len(a))
	for i, v := range a {
		out[i] = v
	}
	return out
}

func stringToInterface(a []string) []interface{} {
	out := make([]interface{}, len(a))
	for i, v := range a {
		out[i] = v
	}
	return out
}

func applyWidths(row []string, widths []int) []string {
	for i, s := range row {
		row[i] = s + lenOffset(s, widths[i])
	}
	return row
}

func lenOffset(s string, w int) string {
	l := w - len(s)
	if l <= 0 {
		return ""
	}
	return strings.Repeat(" ", l)
}

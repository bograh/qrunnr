package admin

import (
	"bufio"
	"bytes"
	"context"
	"io"
	"strings"

	"github.com/a-h/templ"
)

// writeSSE renders component and writes it as one server-sent event. Every
// line of the HTML becomes its own "data:" field, as the SSE format requires.
func writeSSE(ctx context.Context, w io.Writer, event string, component templ.Component) error {
	var html bytes.Buffer
	if err := component.Render(ctx, &html); err != nil {
		return err
	}

	var msg strings.Builder
	msg.WriteString("event: ")
	msg.WriteString(event)
	msg.WriteByte('\n')
	scanner := bufio.NewScanner(&html)
	scanner.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
	wrote := false
	for scanner.Scan() {
		msg.WriteString("data: ")
		msg.WriteString(strings.TrimRight(scanner.Text(), "\r"))
		msg.WriteByte('\n')
		wrote = true
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	if !wrote {
		msg.WriteString("data: \n")
	}
	msg.WriteByte('\n')

	_, err := io.WriteString(w, msg.String())
	return err
}

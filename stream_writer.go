package nbt

import (
	"fmt"
	"io"
)

// StreamWriter provides a streaming interface for writing NBT data incrementally
// without buffering the entire structure in memory.
//
// Basic usage:
//
//	w := nbt.NewStreamWriter(output)
//	w.BeginCompound("Schematic")
//	w.WriteInt32("Version", 3)
//	w.WriteInt16("Width", 256)
//	w.BeginCompound("Blocks")
//	  w.WriteByteArray("Data", blockData)
//	w.EndCompound()
//	w.EndCompound()
//	if err := w.Err(); err != nil {
//	    // handle error
//	}
type StreamWriter struct {
	w        *offsetWriter
	encoding Encoding
	err      error

	// Stack for tracking open compounds/lists
	stack []writerStackEntry
}

type writerStackEntry struct {
	isCompound bool
	listType   tagType
	remaining  int32 // for lists: items remaining to write
}

// NewStreamWriter creates a new streaming NBT writer with NetworkLittleEndian encoding.
func NewStreamWriter(w io.Writer) *StreamWriter {
	return NewStreamWriterWithEncoding(w, NetworkLittleEndian)
}

// NewStreamWriterWithEncoding creates a new streaming NBT writer with a specific encoding.
func NewStreamWriterWithEncoding(w io.Writer, encoding Encoding) *StreamWriter {
	return &StreamWriter{
		w:        newOffsetWriter(w),
		encoding: encoding,
		stack:    make([]writerStackEntry, 0, 16),
	}
}

// Err returns any error encountered during writing.
func (w *StreamWriter) Err() error {
	return w.err
}

// Depth returns the current nesting depth.
func (w *StreamWriter) Depth() int {
	return len(w.stack)
}

// BeginCompound starts writing a named compound tag.
func (w *StreamWriter) BeginCompound(name string) {
	if w.err != nil {
		return
	}
	w.writeTagHeader(tagStruct, name)
	w.stack = append(w.stack, writerStackEntry{isCompound: true})
}

// BeginRootCompound starts writing the root compound tag.
// This is equivalent to BeginCompound but makes the intent clearer.
func (w *StreamWriter) BeginRootCompound(name string) {
	w.BeginCompound(name)
}

// EndCompound finishes writing a compound tag.
func (w *StreamWriter) EndCompound() {
	if w.err != nil {
		return
	}
	if len(w.stack) == 0 || !w.stack[len(w.stack)-1].isCompound {
		w.err = fmt.Errorf("EndCompound called without matching BeginCompound")
		return
	}
	w.stack = w.stack[:len(w.stack)-1]
	w.err = w.w.WriteByte(byte(tagEnd))
}

// BeginList starts writing a named list tag.
func (w *StreamWriter) BeginList(name string, itemType tagType, count int32) {
	if w.err != nil {
		return
	}
	w.writeTagHeader(tagSlice, name)
	if w.err != nil {
		return
	}
	w.err = w.w.WriteByte(byte(itemType))
	if w.err != nil {
		return
	}
	w.err = w.encoding.WriteInt32(w.w, count)
	if w.err != nil {
		return
	}
	w.stack = append(w.stack, writerStackEntry{
		isCompound: false,
		listType:   itemType,
		remaining:  count,
	})
}

// EndList finishes writing a list tag.
// Panics if the list hasn't been fully written.
func (w *StreamWriter) EndList() {
	if w.err != nil {
		return
	}
	if len(w.stack) == 0 || w.stack[len(w.stack)-1].isCompound {
		w.err = fmt.Errorf("EndList called without matching BeginList")
		return
	}
	top := w.stack[len(w.stack)-1]
	if top.remaining > 0 {
		w.err = fmt.Errorf("EndList called with %d items remaining", top.remaining)
		return
	}
	w.stack = w.stack[:len(w.stack)-1]
}

// writeTagHeader writes the tag type and name (for named tags).
func (w *StreamWriter) writeTagHeader(t tagType, name string) {
	if w.err != nil {
		return
	}

	// Check if we're inside a list - list items don't have headers
	if len(w.stack) > 0 && !w.stack[len(w.stack)-1].isCompound {
		top := &w.stack[len(w.stack)-1]
		if top.remaining <= 0 {
			w.err = fmt.Errorf("writing to list with no remaining items")
			return
		}
		if top.listType != t {
			w.err = fmt.Errorf("list expects %v, got %v", top.listType, t)
			return
		}
		top.remaining--
		return // List items don't have tag type or name
	}

	// Write tag type
	if err := w.w.WriteByte(byte(t)); err != nil {
		w.err = err
		return
	}

	// Write tag name
	w.err = w.encoding.WriteString(w.w, name)
}

// WriteByte writes a named byte tag.
func (w *StreamWriter) WriteByte(name string, value byte) {
	w.writeTagHeader(tagByte, name)
	if w.err != nil {
		return
	}
	w.err = w.w.WriteByte(value)
}

// WriteInt16 writes a named short tag.
func (w *StreamWriter) WriteInt16(name string, value int16) {
	w.writeTagHeader(tagInt16, name)
	if w.err != nil {
		return
	}
	w.err = w.encoding.WriteInt16(w.w, value)
}

// WriteInt32 writes a named int tag.
func (w *StreamWriter) WriteInt32(name string, value int32) {
	w.writeTagHeader(tagInt32, name)
	if w.err != nil {
		return
	}
	w.err = w.encoding.WriteInt32(w.w, value)
}

// WriteInt64 writes a named long tag.
func (w *StreamWriter) WriteInt64(name string, value int64) {
	w.writeTagHeader(tagInt64, name)
	if w.err != nil {
		return
	}
	w.err = w.encoding.WriteInt64(w.w, value)
}

// WriteFloat32 writes a named float tag.
func (w *StreamWriter) WriteFloat32(name string, value float32) {
	w.writeTagHeader(tagFloat32, name)
	if w.err != nil {
		return
	}
	w.err = w.encoding.WriteFloat32(w.w, value)
}

// WriteFloat64 writes a named double tag.
func (w *StreamWriter) WriteFloat64(name string, value float64) {
	w.writeTagHeader(tagFloat64, name)
	if w.err != nil {
		return
	}
	w.err = w.encoding.WriteFloat64(w.w, value)
}

// WriteString writes a named string tag.
func (w *StreamWriter) WriteString(name string, value string) {
	w.writeTagHeader(tagString, name)
	if w.err != nil {
		return
	}
	w.err = w.encoding.WriteString(w.w, value)
}

// WriteByteArray writes a named byte array tag.
func (w *StreamWriter) WriteByteArray(name string, value []byte) {
	w.writeTagHeader(tagByteArray, name)
	if w.err != nil {
		return
	}
	w.err = w.encoding.WriteInt32(w.w, int32(len(value)))
	if w.err != nil {
		return
	}
	_, w.err = w.w.Write(value)
}

// WriteInt32Array writes a named int array tag.
func (w *StreamWriter) WriteInt32Array(name string, value []int32) {
	w.writeTagHeader(tagInt32Array, name)
	if w.err != nil {
		return
	}
	w.err = w.encoding.WriteInt32Slice(w.w, value)
}

// WriteInt64Array writes a named long array tag.
func (w *StreamWriter) WriteInt64Array(name string, value []int64) {
	w.writeTagHeader(tagInt64Array, name)
	if w.err != nil {
		return
	}
	w.err = w.encoding.WriteInt64Slice(w.w, value)
}

// --- List item writers (no name parameter) ---

// WriteListByte writes a byte value as a list item.
func (w *StreamWriter) WriteListByte(value byte) {
	w.writeTagHeader(tagByte, "")
	if w.err != nil {
		return
	}
	w.err = w.w.WriteByte(value)
}

// WriteListInt16 writes a short value as a list item.
func (w *StreamWriter) WriteListInt16(value int16) {
	w.writeTagHeader(tagInt16, "")
	if w.err != nil {
		return
	}
	w.err = w.encoding.WriteInt16(w.w, value)
}

// WriteListInt32 writes an int value as a list item.
func (w *StreamWriter) WriteListInt32(value int32) {
	w.writeTagHeader(tagInt32, "")
	if w.err != nil {
		return
	}
	w.err = w.encoding.WriteInt32(w.w, value)
}

// WriteListInt64 writes a long value as a list item.
func (w *StreamWriter) WriteListInt64(value int64) {
	w.writeTagHeader(tagInt64, "")
	if w.err != nil {
		return
	}
	w.err = w.encoding.WriteInt64(w.w, value)
}

// WriteListFloat32 writes a float value as a list item.
func (w *StreamWriter) WriteListFloat32(value float32) {
	w.writeTagHeader(tagFloat32, "")
	if w.err != nil {
		return
	}
	w.err = w.encoding.WriteFloat32(w.w, value)
}

// WriteListFloat64 writes a double value as a list item.
func (w *StreamWriter) WriteListFloat64(value float64) {
	w.writeTagHeader(tagFloat64, "")
	if w.err != nil {
		return
	}
	w.err = w.encoding.WriteFloat64(w.w, value)
}

// WriteListString writes a string value as a list item.
func (w *StreamWriter) WriteListString(value string) {
	w.writeTagHeader(tagString, "")
	if w.err != nil {
		return
	}
	w.err = w.encoding.WriteString(w.w, value)
}

// BeginListCompound starts writing a compound as a list item.
func (w *StreamWriter) BeginListCompound() {
	w.writeTagHeader(tagStruct, "")
	if w.err != nil {
		return
	}
	w.stack = append(w.stack, writerStackEntry{isCompound: true})
}

// Offset returns the current byte offset in the stream.
func (w *StreamWriter) Offset() int64 {
	return w.w.off
}

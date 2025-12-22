package nbt

import (
	"fmt"
	"io"
)

// StreamReader provides a streaming interface for reading NBT data without fully
// materializing the tree into memory. It iterates over tags token-by-token,
// allowing callers to read specific fields or skip entire subtrees.
//
// Basic usage:
//
//	reader := nbt.NewStreamReader(r)
//	for reader.Next() {
//	    if reader.TagName() == "BlockData" && reader.TagType() == nbt.TagByteArray {
//	        data, _ := reader.ByteArray()
//	        // process data...
//	    } else {
//	        reader.Skip()
//	    }
//	}
//	if err := reader.Err(); err != nil {
//	    // handle error
//	}
type StreamReader struct {
	r        *offsetReader
	encoding Encoding
	err      error

	// Current tag state
	tagType tagType
	tagName string
	depth   int

	// Stack for tracking compound/list nesting
	stack []stackEntry
}

type stackEntry struct {
	isCompound bool
	listType   tagType
	remaining  int64
}

// NewScanner creates a new streaming NBT scanner with NetworkLittleEndian encoding.
func NewStreamReader(r io.Reader) *StreamReader {
	return NewStreamReaderWithEncoding(r, NetworkLittleEndian)
}

// NewStreamReaderWithEncoding creates a new streaming NBT scanner with a specific encoding.
func NewStreamReaderWithEncoding(r io.Reader, encoding Encoding) *StreamReader {
	return &StreamReader{
		r:        newOffsetReader(r),
		encoding: encoding,
		stack:    make([]stackEntry, 0, 16),
	}
}

// Next advances the scanner to the next tag.
// Returns true if a tag was found, false if EOF or error.
// After Next returns false, call Err() to check for errors.
func (s *StreamReader) Next() bool {
	if s.err != nil {
		return false
	}

	// Handle list item iteration
	if len(s.stack) > 0 {
		top := &s.stack[len(s.stack)-1]
		if !top.isCompound {
			// Inside a list
			if top.remaining > 0 {
				top.remaining--
				s.tagType = top.listType
				s.tagName = ""
				return true
			}
			// List exhausted, pop and continue
			s.stack = s.stack[:len(s.stack)-1]
		}
	}

	// Read next tag
	tagTypeByte, err := s.r.ReadByte()
	if err != nil {
		if err == io.EOF {
			s.err = nil // Normal EOF
		} else {
			s.err = err
		}
		return false
	}
	s.tagType = tagType(tagTypeByte)

	// TAG_End exits current compound
	if s.tagType == tagEnd {
		if len(s.stack) > 0 && s.stack[len(s.stack)-1].isCompound {
			s.stack = s.stack[:len(s.stack)-1]
		}
		// Return false to signal this compound has ended
		// The caller's for loop will exit, and the outer caller will call Next() again
		return false
	}

	// Read tag name
	s.tagName, s.err = s.encoding.String(s.r)
	if s.err != nil {
		return false
	}

	return true
}

// Err returns any error encountered during scanning.
func (s *StreamReader) Err() error {
	return s.err
}

// TagType returns the type of the current tag.
func (s *StreamReader) TagType() tagType {
	return s.tagType
}

// TagName returns the name of the current tag.
func (s *StreamReader) TagName() string {
	return s.tagName
}

// Depth returns the current nesting depth.
func (s *StreamReader) Depth() int {
	return len(s.stack)
}

// Skip skips the current tag's value without reading it.
// For compounds and lists, this skips the entire subtree.
func (s *StreamReader) Skip() error {
	if s.err != nil {
		return s.err
	}
	s.err = s.skipValue(s.tagType)
	return s.err
}

// Byte reads a TAG_Byte value.
func (s *StreamReader) Byte() (byte, error) {
	if s.tagType != tagByte {
		return 0, fmt.Errorf("expected TAG_Byte, got %v", s.tagType)
	}
	return s.r.ReadByte()
}

// Int16 reads a TAG_Short value.
func (s *StreamReader) Int16() (int16, error) {
	if s.tagType != tagInt16 {
		return 0, fmt.Errorf("expected TAG_Short, got %v", s.tagType)
	}
	return s.encoding.Int16(s.r)
}

// Int32 reads a TAG_Int value.
func (s *StreamReader) Int32() (int32, error) {
	if s.tagType != tagInt32 {
		return 0, fmt.Errorf("expected TAG_Int, got %v", s.tagType)
	}
	return s.encoding.Int32(s.r)
}

// Int64 reads a TAG_Long value.
func (s *StreamReader) Int64() (int64, error) {
	if s.tagType != tagInt64 {
		return 0, fmt.Errorf("expected TAG_Long, got %v", s.tagType)
	}
	return s.encoding.Int64(s.r)
}

// Float32 reads a TAG_Float value.
func (s *StreamReader) Float32() (float32, error) {
	if s.tagType != tagFloat32 {
		return 0, fmt.Errorf("expected TAG_Float, got %v", s.tagType)
	}
	return s.encoding.Float32(s.r)
}

// Float64 reads a TAG_Double value.
func (s *StreamReader) Float64() (float64, error) {
	if s.tagType != tagFloat64 {
		return 0, fmt.Errorf("expected TAG_Double, got %v", s.tagType)
	}
	return s.encoding.Float64(s.r)
}

// String reads a TAG_String value.
func (s *StreamReader) String() (string, error) {
	if s.tagType != tagString {
		return "", fmt.Errorf("expected TAG_String, got %v", s.tagType)
	}
	return s.encoding.String(s.r)
}

// ByteArray reads a TAG_ByteArray value.
func (s *StreamReader) ByteArray() ([]byte, error) {
	if s.tagType != tagByteArray {
		return nil, fmt.Errorf("expected TAG_ByteArray, got %v", s.tagType)
	}
	length, err := s.encoding.Int32(s.r)
	if err != nil {
		return nil, err
	}
	if length < 0 {
		return nil, BufferOverrunError{Op: "ByteArray"}
	}
	data := make([]byte, length)
	if _, err := io.ReadFull(s.r, data); err != nil {
		return nil, BufferOverrunError{Op: "ByteArray"}
	}
	return data, nil
}

// Int32Array reads a TAG_IntArray value.
func (s *StreamReader) Int32Array() ([]int32, error) {
	if s.tagType != tagInt32Array {
		return nil, fmt.Errorf("expected TAG_IntArray, got %v", s.tagType)
	}
	return s.encoding.Int32Slice(s.r)
}

// Int64Array reads a TAG_LongArray value.
func (s *StreamReader) Int64Array() ([]int64, error) {
	if s.tagType != tagInt64Array {
		return nil, fmt.Errorf("expected TAG_LongArray, got %v", s.tagType)
	}
	return s.encoding.Int64Slice(s.r)
}

// EnterCompound enters a TAG_Compound to iterate its children.
// After calling this, use Next() to iterate through child tags.
// The compound is automatically exited when TAG_End is encountered.
func (s *StreamReader) EnterCompound() error {
	if s.tagType != tagStruct {
		return fmt.Errorf("expected TAG_Compound, got %v", s.tagType)
	}
	s.stack = append(s.stack, stackEntry{isCompound: true})
	return nil
}

// ListInfo reads the type and length of a TAG_List without entering it.
// Use EnterList() to actually iterate the list items.
func (s *StreamReader) ListInfo() (itemType tagType, length int32, err error) {
	if s.tagType != tagSlice {
		return 0, 0, fmt.Errorf("expected TAG_List, got %v", s.tagType)
	}
	typeByte, err := s.r.ReadByte()
	if err != nil {
		return 0, 0, err
	}
	length, err = s.encoding.Int32(s.r)
	if err != nil {
		return 0, 0, err
	}
	return tagType(typeByte), length, nil
}

// EnterList reads list metadata and enters a TAG_List to iterate its items.
// Returns the item type and count. After calling this, use Next() to get each item.
func (s *StreamReader) EnterList() (itemType tagType, count int32, err error) {
	itemType, count, err = s.ListInfo()
	if err != nil {
		return 0, 0, err
	}
	if count > 0 {
		s.stack = append(s.stack, stackEntry{
			isCompound: false,
			listType:   itemType,
			remaining:  int64(count),
		})
	}
	return itemType, count, nil
}

// Value reads and returns the value of the current primitive tag.
// For compounds/lists, use EnterCompound/EnterList instead.
func (s *StreamReader) Value() (any, error) {
	switch s.tagType {
	case tagByte:
		return s.Byte()
	case tagInt16:
		return s.Int16()
	case tagInt32:
		return s.Int32()
	case tagInt64:
		return s.Int64()
	case tagFloat32:
		return s.Float32()
	case tagFloat64:
		return s.Float64()
	case tagString:
		return s.String()
	case tagByteArray:
		return s.ByteArray()
	case tagInt32Array:
		return s.Int32Array()
	case tagInt64Array:
		return s.Int64Array()
	default:
		return nil, fmt.Errorf("cannot read value of %v", s.tagType)
	}
}

// ReadCompound reads an entire compound tag into a map.
// This is a convenience method for when you need the whole compound in memory.
func (s *StreamReader) ReadCompound() (map[string]any, error) {
	if s.tagType != tagStruct {
		return nil, fmt.Errorf("expected TAG_Compound, got %v", s.tagType)
	}

	result := make(map[string]any)
	if err := s.EnterCompound(); err != nil {
		return nil, err
	}

	for s.Next() {
		if s.Depth() == 0 {
			break // Exited the compound
		}

		name := s.tagName
		switch s.tagType {
		case tagStruct:
			val, err := s.ReadCompound()
			if err != nil {
				return nil, err
			}
			result[name] = val
		case tagSlice:
			val, err := s.ReadList()
			if err != nil {
				return nil, err
			}
			result[name] = val
		default:
			val, err := s.Value()
			if err != nil {
				return nil, err
			}
			result[name] = val
		}
	}

	return result, s.err
}

// ReadList reads an entire list tag into a slice.
// This is a convenience method for when you need the whole list in memory.
func (s *StreamReader) ReadList() ([]any, error) {
	if s.tagType != tagSlice {
		return nil, fmt.Errorf("expected TAG_List, got %v", s.tagType)
	}

	_, count, err := s.EnterList()
	if err != nil {
		return nil, err
	}

	result := make([]any, 0, count)
	for i := int32(0); i < count && s.Next(); i++ {
		switch s.tagType {
		case tagStruct:
			val, err := s.ReadCompound()
			if err != nil {
				return nil, err
			}
			result = append(result, val)
		case tagSlice:
			val, err := s.ReadList()
			if err != nil {
				return nil, err
			}
			result = append(result, val)
		default:
			val, err := s.Value()
			if err != nil {
				return nil, err
			}
			result = append(result, val)
		}
	}

	return result, s.err
}

// skipValue skips a value of the given type
func (s *StreamReader) skipValue(t tagType) error {
	switch t {
	case tagEnd:
		return nil
	case tagByte:
		_, err := s.r.ReadByte()
		return err
	case tagInt16:
		_, err := s.encoding.Int16(s.r)
		return err
	case tagInt32:
		_, err := s.encoding.Int32(s.r)
		return err
	case tagInt64:
		_, err := s.encoding.Int64(s.r)
		return err
	case tagFloat32:
		_, err := s.encoding.Float32(s.r)
		return err
	case tagFloat64:
		_, err := s.encoding.Float64(s.r)
		return err
	case tagString:
		_, err := s.encoding.String(s.r)
		return err
	case tagByteArray:
		length, err := s.encoding.Int32(s.r)
		if err != nil {
			return err
		}
		return s.skipBytes(int64(length))
	case tagInt32Array:
		length, err := s.encoding.Int32(s.r)
		if err != nil {
			return err
		}
		return s.skipBytes(int64(length) * 4)
	case tagInt64Array:
		length, err := s.encoding.Int32(s.r)
		if err != nil {
			return err
		}
		return s.skipBytes(int64(length) * 8)
	case tagSlice:
		listTypeByte, err := s.r.ReadByte()
		if err != nil {
			return err
		}
		listType := tagType(listTypeByte)
		length, err := s.encoding.Int32(s.r)
		if err != nil {
			return err
		}
		for range length {
			if err := s.skipValue(listType); err != nil {
				return err
			}
		}
		return nil
	case tagStruct:
		for {
			childTypeByte, err := s.r.ReadByte()
			if err != nil {
				return err
			}
			childType := tagType(childTypeByte)
			if childType == tagEnd {
				return nil
			}
			// Skip name
			if _, err := s.encoding.String(s.r); err != nil {
				return err
			}
			if err := s.skipValue(childType); err != nil {
				return err
			}
		}
	default:
		return UnknownTagError{Off: s.r.off, TagType: t, Op: "Skip"}
	}
}

// skipBytes skips n bytes from the reader
func (s *StreamReader) skipBytes(n int64) error {
	if seeker, ok := s.r.Reader.(io.Seeker); ok {
		_, err := seeker.Seek(n, io.SeekCurrent)
		if err == nil {
			s.r.off += n
			return nil
		}
	}
	buf := make([]byte, min(n, 32*1024))
	for n > 0 {
		toRead := min(int64(len(buf)), n)
		read, err := s.r.Read(buf[:toRead])
		if err != nil {
			return err
		}
		n -= int64(read)
	}
	return nil
}

// Offset returns the current byte offset in the stream.
func (s *StreamReader) Offset() int64 {
	return s.r.off
}

// Tag type constants for external use
const (
	TagEnd       = tagEnd
	TagByte      = tagByte
	TagInt16     = tagInt16
	TagInt32     = tagInt32
	TagInt64     = tagInt64
	TagFloat32   = tagFloat32
	TagFloat64   = tagFloat64
	TagByteArray = tagByteArray
	TagString    = tagString
	TagList      = tagSlice
	TagCompound  = tagStruct
	TagIntArray  = tagInt32Array
	TagLongArray = tagInt64Array
)

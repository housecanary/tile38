package rbang

import (
	"encoding/binary"
	"io"
	"reflect"
	"unsafe"
)

func (tr *RTree) Save(f io.Writer, saveValue func(w io.Writer, value interface{}) error) (err error) {
	if err = binary.Write(f, binary.BigEndian, uint64(tr.height)); err != nil {
		return
	}

	if err = binary.Write(f, binary.BigEndian, uint64(tr.count)); err != nil {
		return
	}

	gotTree := tr.root.data != nil
	if err = binary.Write(f, binary.BigEndian, gotTree); err != nil {
		return
	}

	if gotTree {
		if err = tr.root.save(f, saveValue, tr.height); err != nil {
			return
		}
	}

	return
}

func (r *rect) save(f io.Writer,
	saveValue func(w io.Writer, data interface{}) error,
	height int,
) (err error) {
	if _, err = f.Write(floatsAsBytes(r.min[:])); err != nil {
		return
	}
	if _, err = f.Write(floatsAsBytes(r.max[:])); err != nil {
		return
	}

	n := r.data.(*node)
	nItems := uint8(n.count)
	if err = binary.Write(f, binary.BigEndian, nItems); err != nil {
		return
	}

	gotChildren := height > 0
	if err = binary.Write(f, binary.BigEndian, gotChildren); err != nil {
		return
	}

	if gotChildren {
		for i := 0; i < n.count; i++ {
			if err = n.rects[i].save(f, saveValue, height-1); err != nil {
				return
			}
		}
	} else {
		for i := 0; i < n.count; i++ {
			if _, err = f.Write(floatsAsBytes(n.rects[i].min[:])); err != nil {
				return
			}
			if _, err = f.Write(floatsAsBytes(n.rects[i].max[:])); err != nil {
				return
			}
			if err = saveValue(f, n.rects[i].data); err != nil {
				return
			}
		}
	}
	return
}

func (tr *RTree) Load(
	f io.Reader,
	loadValue func(r io.Reader, obuf []byte) (interface{}, []byte, error),
) (err error) {
	var word uint64

	if err = binary.Read(f, binary.BigEndian, &word); err != nil {
		return
	}
	tr.height = int(word)

	if err = binary.Read(f, binary.BigEndian, &word); err != nil {
		return
	}
	tr.count = int(word)

	var gotTree bool
	if err = binary.Read(f, binary.BigEndian, &gotTree); err != nil {
		return
	}

	if gotTree {
		// this buffer will be re-used or replaced for a larger one, as needed
		buf := make([]byte, 32)
		if tr.root, buf, err = load(f, buf, loadValue); err != nil {
			return
		}

		// Temporary code - analyze the fill factor on existing collection
		maxEntries := 0
		var findMaxEntries func(data interface{})

		findMaxEntries = func(data interface{}) {
			if data == nil {
				return
			}

			switch data := data.(type) {
			case *node:
				{
					if data.count > maxEntries {
						maxEntries = data.count
					}

					for x := 0; x < data.count; x++ {
						if data.rects[x].data != nil {
							findMaxEntries(data.rects[x].data)
						}
					}
				}
			}
		}

		findMaxEntries(tr.root.data)

		if maxEntries > tr.GetSplitEntries() {
			tr.SetSplitEntries(maxEntries)
		}
	}

	tr.RecordStats()

	return
}

func load(
	f io.Reader,
	oldBuf []byte,
	loadValue func(r io.Reader, obuf []byte) (interface{}, []byte, error),
) (r rect, buf []byte, err error) {
	buf = oldBuf[:]

	if err = r.setMinMaxFromFile(f, buf); err != nil {
		return
	}

	n := &node{}
	r.data = n

	var short uint8
	if err = binary.Read(f, binary.BigEndian, &short); err != nil {
		return
	}
	n.count = int(short)

	var gotChildren bool
	if err = binary.Read(f, binary.BigEndian, &gotChildren); err != nil {
		return
	}

	if gotChildren {
		for i := 0; i < n.count; i++ {
			if n.rects[i], buf, err = load(f, buf, loadValue); err != nil {
				return
			}
		}
	} else {
		for i := 0; i < n.count; i++ {
			if err = n.rects[i].setMinMaxFromFile(f, buf); err != nil {
				return
			}
			if n.rects[i].data, buf, err = loadValue(f, buf); err != nil {
				return
			}
		}
	}

	return
}

func (r *rect) setMinMaxFromFile(f io.Reader, buf []byte) (err error) {
	buf = buf[:32]
	if _, err = io.ReadFull(f, buf); err != nil {
		return
	}
	floatsMinMax := bytesAsFloats(buf)
	r.min[0] = floatsMinMax[0]
	r.min[1] = floatsMinMax[1]
	r.max[0] = floatsMinMax[2]
	r.max[1] = floatsMinMax[3]

	return
}

func floatsAsBytes(row []float64) []byte {
	header := *(*reflect.SliceHeader)(unsafe.Pointer(&row))
	header.Len *= 8
	header.Cap *= 8
	return *(*[]byte)(unsafe.Pointer(&header))
}

func bytesAsFloats(row []byte) []float64 {
	header := *(*reflect.SliceHeader)(unsafe.Pointer(&row))
	header.Len /= 8
	header.Cap /= 8
	return *(*[]float64)(unsafe.Pointer(&header))
}

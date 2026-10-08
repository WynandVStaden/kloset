package vfs

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/PlakarKorp/kloset/objects"
	"github.com/PlakarKorp/kloset/repository"
	"github.com/PlakarKorp/kloset/resources"
)

const default_prefetchSize = 4 * 1024 * 1024

const (
	// maxReadahead caps a readahead window. A reader holds at most the
	// window it consumes plus the next one in flight.
	maxReadahead = 32 << 20

	// readaheadConcurrency is the number of range reads, of at most 4MB,
	// GetBlobs runs in parallel for a window.
	readaheadConcurrency = 4
)

type ObjectReader struct {
	object *objects.Object
	repo   *repository.Repository
	size   int64

	objoff       int   // Chunk offset at which we currently are reading.
	bufferOffset int64 // Offset inside the chunk we are currently reading.
	off          int64 // Global file offset.

	doSeek bool // Flag to invalidate the prefetchBuffer

	prefetchSize     int32
	prefetchedObjoff int // Keeps track of the chunk position we prefetched up to.
	prefetchBuffer   bytes.Buffer

	// Sequential readahead, see newReadaheadObjectReader. It starts on the
	// first Read; the first Seek after that, or a failed fetch, hands the
	// position back to the plain path above for good.
	readahead bool
	ra        *readahead
}

func NewObjectReader(repo *repository.Repository, object *objects.Object, size int64, prefetchSize int32) *ObjectReader {
	if prefetchSize <= 0 {
		prefetchSize = default_prefetchSize
	}
	return &ObjectReader{
		object:         object,
		repo:           repo,
		size:           size,
		prefetchSize:   prefetchSize,
		prefetchBuffer: bytes.Buffer{},
	}
}

// newReadaheadObjectReader returns an ObjectReader fetching ahead in the
// background while it is read sequentially. It must be closed.
func newReadaheadObjectReader(repo *repository.Repository, object *objects.Object, size int64) *ObjectReader {
	or := NewObjectReader(repo, object, size, -1)
	or.readahead = true
	return or
}

// stopReadahead leaves the readahead path for good, handing its position back
// to the plain path.
func (or *ObjectReader) stopReadahead() {
	or.readahead = false
	if or.ra == nil {
		return
	}

	ra := or.ra
	or.ra = nil
	ra.close()

	// Cannot fail, SeekStart only rejects negative offsets.
	_, _ = or.Seek(ra.off, io.SeekStart)
}

// Close stops the readahead, if any.
func (or *ObjectReader) Close() error {
	or.stopReadahead()
	return nil
}

func (or *ObjectReader) prefetch() error {
	var seekBytes int64
	if or.doSeek {
		or.prefetchBuffer.Reset()
		or.prefetchedObjoff = or.objoff
		seekBytes = or.bufferOffset
	}

	// We are not seeking and we have data in the prefectBuffer just use it.
	if or.prefetchBuffer.Len() != 0 {
		return nil
	}

	// We are past the last chunk, and not seeking so it's the end of the file.
	if or.prefetchedObjoff >= len(or.object.Chunks) {
		return io.EOF
	}

	for data, err := range or.repo.GetObjectContent(or.object, or.prefetchedObjoff, uint32(or.prefetchSize)) {
		if err != nil {
			return err
		}

		if seekBytes != 0 {
			data = data[seekBytes:]
			seekBytes = 0
		}

		if _, err := or.prefetchBuffer.Write(data); err != nil {
			return err
		}
		or.prefetchedObjoff++
	}

	return nil
}

func (or *ObjectReader) Read(p []byte) (int, error) {
	if or.readahead {
		if or.ra == nil {
			or.ra = newReadahead(or.repo, or.object.Chunks, or.objoff, or.bufferOffset, or.off, int(or.prefetchSize))
		}

		n, err := or.ra.Read(p)
		if err != nil && err != io.EOF {
			// A retry goes through the plain path, from where we failed.
			or.stopReadahead()
		}
		return n, err
	}

	if err := or.prefetch(); err != nil {
		return 0, err
	}

	read, err := or.prefetchBuffer.Read(p)
	// (Ab)use Seek to keep the offset and objoff up to date according to the
	// Read we did.
	or.Seek(int64(read), io.SeekCurrent)
	// It is not a real seek, we did not move.
	or.doSeek = false

	// EOF on the prefetchBuffer just means we exhausted it not the underlying
	// file
	if err == io.EOF {
		err = nil
	}

	return read, err
}

func (or *ObjectReader) Seek(offset int64, whence int) (int64, error) {
	if or.ra != nil {
		or.stopReadahead()
	}

	chunks := or.object.Chunks

	switch whence {
	case io.SeekStart:
		if offset < 0 {
			return 0, os.ErrInvalid
		}

		or.off = 0

		for or.objoff = 0; or.objoff < len(chunks); or.objoff++ {
			clen := int64(chunks[or.objoff].Length)
			if offset > clen {
				or.off += clen
				offset -= clen
				continue
			}

			or.bufferOffset = offset
			or.off += offset
			or.doSeek = true

			break
		}

	case io.SeekEnd:
		if offset > 0 {
			return 0, os.ErrInvalid
		}

		if offset == 0 {
			or.objoff = len(chunks)
			or.bufferOffset = 0
			or.off = or.size
			or.doSeek = true
			break
		}

		offset *= -1
		or.off = or.size
		for or.objoff = len(chunks) - 1; or.objoff >= 0; or.objoff-- {
			clen := int64(chunks[or.objoff].Length)
			if offset > clen {
				or.off -= clen
				offset -= clen
				continue
			}
			or.bufferOffset = clen - offset
			or.off -= offset
			or.doSeek = true
			break
		}

	case io.SeekCurrent:
		if offset == 0 {
			break
		}

		if offset > 0 {
			var left int64
			if or.objoff < len(chunks) {
				left = int64(chunks[or.objoff].Length) - or.bufferOffset
			}
			if left > offset {
				or.bufferOffset += offset
				or.off += offset
				or.doSeek = true
				break
			}

			or.off += left
			offset -= left

			if offset == 0 {
				or.objoff = len(chunks)
				or.bufferOffset = 0
				or.off = or.size
				or.doSeek = true
				break
			}

			for or.objoff += 1; or.objoff < len(chunks); or.objoff++ {
				clen := int64(chunks[or.objoff].Length)
				if offset > clen {
					or.off += clen
					offset -= clen
					continue
				}
				or.off += offset
				or.bufferOffset = offset
				or.doSeek = true
				break
			}
		} else {
			offset *= -1

			left := or.bufferOffset
			if left > offset {
				or.bufferOffset -= offset
				or.off -= offset
				or.doSeek = true
				break
			}

			or.off -= left
			offset -= left
			for or.objoff -= 1; or.objoff >= 0; or.objoff-- {
				clen := int64(chunks[or.objoff].Length)
				if offset > clen {
					or.off -= clen
					offset -= clen
					continue
				}
				or.off -= offset
				or.bufferOffset = clen - offset
				or.doSeek = true
				break
			}
		}

	}

	return or.off, nil
}

func (or *ObjectReader) ReadAt(p []byte, off int64) (int, error) {
	if off < 0 {
		return 0, os.ErrInvalid
	}
	if off >= or.size {
		return 0, io.EOF
	}

	cr := NewObjectReader(or.repo, or.object, or.size, or.prefetchSize)
	if _, err := cr.Seek(off, io.SeekStart); err != nil {
		return 0, err
	}

	n, err := io.ReadFull(cr, p)
	if err == io.ErrUnexpectedEOF {
		return n, io.EOF
	}
	return n, err
}

// readahead reads an object sequentially, fetching ahead in the background,
// kernel style: when the reader starts consuming a window, the next one, twice
// as large up to maxReadahead, is already in flight.
type readahead struct {
	repo   *repository.Repository
	chunks []objects.Chunk

	off     int64    // object offset of the next byte Read returns
	skip    int64    // bytes to drop from the first window, when starting mid-chunk
	buf     []byte   // unread part of the current chunk
	pending [][]byte // next chunks of the current window
	size    int      // size of the last window issued
	next    *raWindow
}

// raWindow is the run of chunks [start, end) fetched by its own goroutine.
// data, err and elapsed are valid once done is closed.
type raWindow struct {
	start, end int
	bytes      int

	data    [][]byte
	err     error
	elapsed time.Duration

	cancel context.CancelFunc
	done   chan struct{}
}

func newReadahead(repo *repository.Repository, chunks []objects.Chunk, objoff int, skip, off int64, size int) *readahead {
	ra := &readahead{
		repo:   repo,
		chunks: chunks,
		off:    off,
		skip:   skip,
		size:   size,
	}

	if objoff < len(chunks) {
		ra.next = ra.startWindow(objoff, size)
	}
	return ra
}

func (ra *readahead) Read(p []byte) (int, error) {
	for len(ra.buf) == 0 {
		if len(ra.pending) == 0 {
			if err := ra.advance(); err != nil {
				return 0, err
			}
			continue
		}

		ra.buf = ra.pending[0]
		ra.pending[0] = nil // release consumed chunks as we go
		ra.pending = ra.pending[1:]
	}

	n := copy(p, ra.buf)
	ra.buf = ra.buf[n:]
	ra.off += int64(n)
	return n, nil
}

// advance launches the window after the one in flight, then waits for the
// latter and makes it current.
func (ra *readahead) advance() error {
	w := ra.next
	if w == nil {
		return io.EOF
	}

	ra.next = nil
	if w.end < len(ra.chunks) {
		ra.size = min(2*ra.size, maxReadahead)
		ra.next = ra.startWindow(w.end, ra.size)
	}

	t0 := time.Now()
	<-w.done
	w.cancel()

	ra.repo.Logger().Trace("vfs", "readahead [%d, %d) %d bytes: fetched in %s, waited %s",
		w.start, w.end, w.bytes, w.elapsed, time.Since(t0))

	if w.err != nil {
		return w.err
	}

	ra.pending = w.data
	if ra.skip != 0 {
		ra.pending[0] = ra.pending[0][ra.skip:]
		ra.skip = 0
	}
	return nil
}

// startWindow fetches, in the background, the chunks from start until at
// least size bytes are covered.
func (ra *readahead) startWindow(start, size int) *raWindow {
	end, bytes := start, 0
	for end < len(ra.chunks) && bytes < size {
		bytes += int(ra.chunks[end].Length)
		end++
	}

	ctx, cancel := context.WithCancel(ra.repo.AppContext())
	w := &raWindow{
		start:  start,
		end:    end,
		bytes:  bytes,
		cancel: cancel,
		done:   make(chan struct{}),
	}

	repo, chunks := ra.repo, ra.chunks[start:end]
	go func() {
		defer close(w.done)

		t0 := time.Now()
		w.data, w.err = fetchChunks(ctx, repo, chunks)
		w.elapsed = time.Since(t0)
	}()

	return w
}

// close cancels the window in flight, if any, and waits for its goroutine.
func (ra *readahead) close() {
	if ra.next == nil {
		return
	}

	ra.next.cancel()
	<-ra.next.done
	ra.next = nil
}

// fetchChunks returns the decoded chunks, in order, fetched as one GetBlobs
// batch.
func fetchChunks(ctx context.Context, repo *repository.Repository, chunks []objects.Chunk) ([][]byte, error) {
	reqs := make([]repository.BlobReq, 0, len(chunks))
	for _, c := range chunks {
		reqs = append(reqs, repository.BlobReq{Type: resources.RT_CHUNK, MAC: c.ContentMAC})
	}

	blobs := make(map[objects.MAC][]byte, len(chunks))
	opts := &repository.GetBlobsOpts{Concurrency: readaheadConcurrency}
	for b, err := range repo.GetBlobs(ctx, reqs, opts) {
		if err != nil {
			return nil, fmt.Errorf("chunk %x: %w", b.MAC, err)
		}
		blobs[b.MAC] = b.Data
	}

	// GetBlobs stops early, without an error, when ctx is cancelled.
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	// GetBlobs dedups: a chunk repeated in the window comes back once.
	data := make([][]byte, 0, len(chunks))
	for _, c := range chunks {
		d, ok := blobs[c.ContentMAC]
		if !ok {
			return nil, fmt.Errorf("chunk %x: %w", c.ContentMAC, repository.ErrBlobNotFound)
		}
		data = append(data, d)
	}

	return data, nil
}

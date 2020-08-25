using System;
using System.IO;
using System.IO.MemoryMappedFiles;
using System.Runtime.CompilerServices;
using Microsoft.Win32.SafeHandles;

namespace Aeron.MediaDriver.Native
{
    internal unsafe class FixedSizeDirectFile : IDisposable
    {
        private readonly string _filePath;
        private readonly FileStream _fileStream;
        private readonly MemoryMappedFile _mmf;
        private readonly MemoryMappedViewAccessor _va;
        private readonly SafeMemoryMappedViewHandle _vaHandle;

        private byte* _pointer;
        private int _size;

        public FixedSizeDirectFile(string filePath, int size = 4096)
        {
            _filePath = filePath;

            if (size < 0)
                throw new ArgumentOutOfRangeException(nameof(size));

            size = ((size - 1) / 4096 + 1) * 4096;

            _fileStream = new FileStream(_filePath,
                                         FileMode.OpenOrCreate,
                                         FileAccess.ReadWrite,
                                         FileShare.ReadWrite,
                                         1,
                                         FileOptions.None);

            var bytesCapacity = checked((int)Math.Max(_fileStream.Length, size));

            _mmf = MemoryMappedFile.CreateFromFile(_fileStream,
                                                   null,
                                                   bytesCapacity,
                                                   MemoryMappedFileAccess.ReadWrite,
                                                   HandleInheritability.None,
                                                   true);

            _va = _mmf.CreateViewAccessor(0, bytesCapacity, MemoryMappedFileAccess.ReadWrite);
            _vaHandle = _va.SafeMemoryMappedViewHandle;
            _vaHandle.AcquirePointer(ref _pointer);

            _size = bytesCapacity;
        }

        public Span<byte> Span
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => new Span<byte>(_pointer, _size);
        }

        public long Length
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => _size;
        }

        public void Dispose()
        {
            Flush(true);

            _vaHandle.ReleasePointer();
            _va.Dispose();
            _mmf.Dispose();
            _fileStream.Dispose();
            _pointer = null;
            _size = 0;
        }

        public void Flush(bool flushToDisk = false)
        {
            _va.Flush();

            if (flushToDisk)
                _fileStream.Flush(true);
        }

        public string FilePath
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => _filePath;
        }
    }
}

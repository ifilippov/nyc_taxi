#ifndef LOAD_H
#define LOAD_H

#include <arrow/api.h>
#include <arrow/csv/reader.h>
#include <arrow/io/file.h>
#include <tbb/scalable_allocator.h>
#include <print.h>

using namespace arrow;

class TBBMemoryPool : public MemoryPool {
 public:
  TBBMemoryPool() {
    if( TBBMALLOC_NO_EFFECT == scalable_allocation_mode(TBBMALLOC_USE_HUGE_PAGES, 1) )
      printf("Warning: TBB cannot enable huge pages, try `sudo bash -c 'echo 30000 > /proc/sys/vm/nr_hugepages'`\n");
  }
  ~TBBMemoryPool() override {
    scalable_allocation_command(TBBMALLOC_CLEAN_ALL_BUFFERS, nullptr);
  }

  Status Allocate(int64_t size, uint8_t** out) override {
    *out = (uint8_t*)scalable_aligned_malloc(size, 64);
    if(!*out)
      return Status::OutOfMemory("malloc of size", size, "failed");
    stats_.UpdateAllocatedBytes(size);
    return Status::OK();
  }

  Status Reallocate(int64_t old_size, int64_t new_size, uint8_t** ptr) override {
    *ptr = (uint8_t*)scalable_aligned_realloc(*ptr, new_size, 64);
    if(!*ptr)
      return Status::OutOfMemory("malloc of size", new_size, "failed");
    stats_.UpdateAllocatedBytes(new_size - old_size);
    return Status::OK();
  }

  int64_t bytes_allocated() const override { return stats_.bytes_allocated(); }

  void Free(uint8_t* buffer, int64_t size) override {
    scalable_aligned_free(buffer);

    stats_.UpdateAllocatedBytes(-size);
  }

  int64_t max_memory() const override { return stats_.max_memory(); }

 private:
  internal::MemoryPoolStats stats_;
};


//++++++++++++++++++++++++++++++
// LOAD CSV
//++++++++++++++++++++++++++++++
std::shared_ptr<arrow::Table> load_csv(std::string path, bool threads, int bsz = 1*1024*1024) {
    printf("TASK: loading CSV file using %s.\n", threads ? "multiple threads" : "single thread");
    auto begin = std::chrono::steady_clock::now();

    auto popt = arrow::csv::ParseOptions::Defaults();
    popt.quoting = false;
    popt.newlines_in_values = false;

    auto ropt = arrow::csv::ReadOptions::Defaults();
    ropt.use_threads = threads;
    ropt.block_size = bsz;

    auto copt = arrow::csv::ConvertOptions::Defaults();
#if BENCH_USE_TBB_HUGE_PAGES
    auto memup = std::unique_ptr<MemoryPool>(new TBBMemoryPool);
    auto memp = memup.get();
#else
    auto memp = arrow::default_memory_pool();
#endif

    std::shared_ptr<arrow::io::ReadableFile> inp;
    auto r = arrow::io::ReadableFile::Open(path, &inp); // TODO check existence

    std::shared_ptr<arrow::csv::TableReader> trp;
    r = arrow::csv::TableReader::Make(memp, inp, ropt, popt, copt, &trp);

    std::shared_ptr<arrow::Table> out;
    r = trp->Read(&out);
    print_time(begin);
    return out;
}

#endif

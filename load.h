#ifndef LOAD_H
#define LOAD_H

#include <arrow/api.h>
#include <arrow/csv/reader.h>
#include <arrow/io/file.h>

#include <print.h>

//++++++++++++++++++++++++++++++
// LOAD CSV
//++++++++++++++++++++++++++++++

std::shared_ptr<arrow::Table> load_csv(std::string path, bool threads) {
        printf("TASK: loading CSV file using %s.\n", threads ? "multiple threads" : "single thread");
	auto begin = std::chrono::steady_clock::now();

	auto popt = arrow::csv::ParseOptions::Defaults();
	auto ropt = arrow::csv::ReadOptions::Defaults();
	ropt.use_threads = threads;
	auto copt = arrow::csv::ConvertOptions::Defaults();
	auto memp = arrow::MemoryPool::CreateDefault();

 	std::shared_ptr<arrow::io::ReadableFile> inp;
	auto r = arrow::io::ReadableFile::Open(path,&inp);

	std::shared_ptr<arrow::csv::TableReader> tp;
	r = arrow::csv::TableReader::Make(memp.get(), inp, ropt, popt, copt, &tp);

	std::shared_ptr<arrow::Table> out;
	r = tp->Read(&out);
	print_time(begin);
	return out;
}

#endif

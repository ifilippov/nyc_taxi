#ifndef PRINT_H
#define PRINT_H

#include <arrow/api.h>

#include <arrow/pretty_print.h>
#include <iostream>
#include <chrono>

//++++++++++++++++++++++++++++++
// PRINT
//++++++++++++++++++++++++++++++

void print(std::shared_ptr<arrow::Table> table, bool schema, int N) {
	if (schema) {
		// Print vertically
		printf("Data schema:\n%s\n", table->schema()->ToString().c_str());
	}
	printf("Number of fields/columns:              %d\n", table->schema()->num_fields());
	printf("Number of rows:                        %ld\n", table->num_rows());
	if (N != -1) {
		printf("Field/column %d name:          %s\n", N, table->schema()->field(N)->ToString().c_str());
		printf("Number of chunks in column %d: %d\n", N, table->column(N)->data()->num_chunks());
		printf("Chunk (array) 1 of column  %d: \n%s\n", N, table->column(N)->data()->chunk(0)->ToString().c_str());
	}
}

// Print content horizontally (PrettyPrint was changed - "\n" after each element was removed)
void print_all(std::shared_ptr<arrow::Table> table) {
	std::stringstream ss;
	PrettyPrint(*table, arrow::PrettyPrintOptions(0), &ss);
	std::cout << ss.str();
}

void print_time(std::chrono::steady_clock::time_point begin, std::string line) {
	std::chrono::steady_clock::time_point end = std::chrono::steady_clock::now();
	std::cout << "FOR " << line <<
		std::chrono::duration_cast<std::chrono::seconds>(end - begin).count() << " seconds / " <<
		std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count() << " milliseconds" << std::endl;
}

void print_time(std::chrono::steady_clock::time_point begin) {
	std::chrono::steady_clock::time_point end = std::chrono::steady_clock::now();
	std::cout << "DONE! " <<
		std::chrono::duration_cast<std::chrono::seconds>(end - begin).count() << " seconds / " <<
		std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count() << " milliseconds" << std::endl << std::endl;
}


void print_table(std::shared_ptr<arrow::Table> table) {
	int Nc = table->schema()->num_fields();
	int Nr = table->num_rows();
	printf("***RESULT***\n");
	for (int i = 0; i < Nr; i++) {
		for (int j = 0; j < Nc; j++) {
			auto c = table->column(j)->data();
			if (c->type()->id() == arrow::Type::STRING) {
				auto array = std::static_pointer_cast<arrow::StringArray>(c->chunk(0));
				printf("%s ", array->GetString(i).c_str());
			} else if (c->type()->id() == arrow::Type::INT64) {
				auto array = std::static_pointer_cast<arrow::Int64Array>(c->chunk(0));
				printf("%10ld ", array->Value(i));
			} else {
				auto array = std::static_pointer_cast<arrow::DoubleArray>(c->chunk(0));
				printf("%10f ", array->Value(i));
			}
		}
		printf("\n");
	}
	printf("\n");
}

#endif

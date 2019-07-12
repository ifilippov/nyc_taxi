#ifndef SORT_H
#define SORT_H

#include <arrow/api.h>

#include <print.h>

//++++++++++++++++++++++++++++++
// SORT
//++++++++++++++++++++++++++++++

// For both: single and multiple columns
enum sort_direction {desc, asc};

enum sort_type {flat, tree};

struct index123 {
	int chunkI;
	int elemI;
	static std::vector<int> column_ids;
	static std::vector<sort_direction> t;
	static std::shared_ptr<arrow::Table> table;
};

template <typename T, typename T2>
std::vector<T> *build_vector(std::shared_ptr<arrow::ChunkedArray> column, std::vector<index123>* r) {
	std::vector<T> *result = new std::vector<T>(r->size());
	for (int i = 0; i < r->size(); i++) { // TODO only for one chunk currently
		auto *array = (T2*)column->chunk((*r)[i].chunkI).get();
		(*result)[i] = get_value<T, T2>(array, (*r)[i].elemI);
	}
	return result;
}

std::shared_ptr<arrow::Table> sort_finalize(std::shared_ptr<arrow::Table> table, std::vector<index123>* r) {
	std::vector<std::shared_ptr<arrow::Column>> clmns;
	std::vector<std::shared_ptr<arrow::Field>> flds;
	for (int i = 0; i < table->schema()->num_fields(); i++) {
		auto column = table->column(i);
		std::shared_ptr<arrow::Array> data;
		if (column->type()->id() == arrow::Type::STRING) {
			auto new_column = build_vector<std::string, arrow::StringArray>(column->data(), r);
			data = vector_to_array<std::string, arrow::StringBuilder>(*new_column);
		} else if (column->type()->id() == arrow::Type::INT64) {
			auto new_column = build_vector<arrow::Int64Type::c_type, arrow::Int64Array>(column->data(), r);
			data = vector_to_array<arrow::Int64Type::c_type, arrow::Int64Builder>(*new_column);
		} else {
			auto new_column = build_vector<arrow::DoubleType::c_type, arrow::DoubleArray>(column->data(), r);
			data = vector_to_array<arrow::DoubleType::c_type, arrow::DoubleBuilder>(*new_column);
		}
		auto field = column->field();
		clmns.push_back(std::make_shared<arrow::Column>(field->name(), data));
		flds.push_back(field);
	}
	return arrow::Table::Make(std::make_shared<arrow::Schema>(flds), clmns);
}

// For multiple columns
std::vector<int> index123::column_ids;
std::vector<sort_direction> index123::t;
std::shared_ptr<arrow::Table> index123::table;


bool index123_compare(index123 a, index123 b) {
	for (int i = 0; i < index123::column_ids.size(); i++) {
		auto *arrayA = index123::table->column(index123::column_ids[i])->data()->chunk(a.chunkI).get();
		auto *arrayB = index123::table->column(index123::column_ids[i])->data()->chunk(b.chunkI).get();
		int result = compare(arrayA, a.elemI, arrayB, b.elemI);

		if (result < 0) {
			return index123::t[i] == desc ? false : true;
		} else if (result > 0) {
			return index123::t[i] == desc ? true : false;
		}
	}
	return false;
}

std::vector<index123> *sort_sequential_multiple(int array_length, int chunk_number) {
	std::vector<index123> *result = new std::vector<index123>(array_length);
	// TODO array_to_vector?
	for (int i = 0; i < array_length; i++) {
		(*result)[i] = index123{chunk_number, i};
	}
	std::sort(result->begin(), result->end(), index123_compare);
	return result;
}

std::vector<index123> *sort_parallel_multiple_flat(std::shared_ptr<arrow::Table> table, std::vector<int> column_ids) {
	std::vector<index123> *result = new std::vector<index123>(0);
	for (int i = 0; i < table->column(column_ids[0])->data()->num_chunks(); i++) { // other columns are the same
		// TBB in parallel for all available chunks or sequential for each incoming chunk
		std::vector<index123> *addition = sort_sequential_multiple(table->column(column_ids[0])->data()->chunk(i)->length(), i);
		std::vector<index123> *new_result = new std::vector<index123>(result->size() + addition->size());
		std::merge(result->begin(), result->end(), addition->begin(), addition->end(), new_result->begin(), index123_compare);
		delete(result);
		delete(addition);
		result = new_result;
	}
	return result;
}

std::vector<index123> *sort_parallel_multiple_tree(std::shared_ptr<arrow::Table> table, int a, int b) {
	std::vector<index123> *result;
	if (b - a == 1) {
		// TBB in parallel for all available chunks or sequential for each incoming chunk
		result = sort_sequential_multiple(table->column(index123::column_ids[0])->data()->chunk(a)->length(), a);
	} else {
		auto left = sort_parallel_multiple_tree(table, a, a + (b-a)/2);
		auto right = sort_parallel_multiple_tree(table, a + (b-a)/2, b);
		result = new std::vector<index123>(left->size() + right->size());
		// TBB in parallel for all available chunks or sequential for each incoming chunk
		std::merge(left->begin(), left->end(), right->begin(), right->end(), result->begin(), index123_compare);
		delete(left);
		delete(right);
	}
	return result;
}

std::shared_ptr<arrow::Table> sort_parallel_multiple(std::shared_ptr<arrow::Table> table, std::vector<int> column_ids, std::vector<sort_direction> t, sort_type type) {
	printf("      Arrow is columnar database and this request is low performance\n");
	index123::column_ids = column_ids;
	index123::t = t;
	index123::table = table;
	std::vector<index123> *result;
	if (type == tree) {
		result = sort_parallel_multiple_tree(table, 0, table->column(column_ids[0])->data()->num_chunks());
	} else {
		result = sort_parallel_multiple_flat(table, column_ids);
	}
	auto temp = sort_finalize(table, result);
	delete(result);
	return temp;
}

// For single column
template <typename T>
struct tuple {
	T elem;
	index123 index;
};

template <typename T>
bool tuple_compare_desc(tuple<T> a, tuple<T> b) {
	return a.elem > b.elem;
}

template <typename T>
bool tuple_compare_asc(tuple<T> a, tuple<T> b) {
	return a.elem < b.elem;
}

template <typename T, typename T2>
std::vector<tuple<T>> *sort_sequential_single(std::shared_ptr<T2> array, int chunk_number, sort_direction t) {
	std::vector<tuple<T>> *result = new std::vector<tuple<T>>(array->length());
	// TODO array_to_vector?
	for (int i = 0; i < array->length(); i++) {
		(*result)[i] = tuple<T>{get_value<T, T2>(array.get(), i), index123{chunk_number, i}};
	}
	std::sort(result->begin(), result->end(), t == desc ? tuple_compare_desc<T> : tuple_compare_asc<T>);
	return result;
}

template <typename T, typename T2>
std::vector<tuple<T>> *sort_parallel_single_flat(std::shared_ptr<arrow::ChunkedArray> column, sort_direction t) {
        std::vector<tuple<T>> *result = new std::vector<tuple<T>>(0);
        for (int i = 0; i < column->num_chunks(); i++) {
                auto array = std::static_pointer_cast<T2>(column->chunk(i));
                // TBB in parallel for all available chunks or sequential for each incoming chunk
                std::vector<tuple<T>> *addition = sort_sequential_single<T, T2>(array, i, t);
                std::vector<tuple<T>> *new_result = new std::vector<tuple<T>>(result->size() + addition->size());
                std::merge(result->begin(), result->end(), addition->begin(), addition->end(), new_result->begin(), t == desc ? tuple_compare_desc<T> : tuple_compare_asc<T>);
                delete(result);
                delete(addition);
                result = new_result;
        }
	return result;
}

template <typename T, typename T2>
std::vector<tuple<T>> *sort_parallel_single_tree(std::shared_ptr<arrow::ChunkedArray> column, int a, int b, sort_direction t) {
        std::vector<tuple<T>> *result;
	if (b - a == 1) {
		auto array = std::static_pointer_cast<T2>(column->chunk(a));
		// TBB in parallel for all available chunks or sequential for each incoming chunk
		result = sort_sequential_single<T, T2>(array, a, t);
        } else {
		auto left = sort_parallel_single_tree<T, T2>(column, a, a + (b-a)/2, t);
                auto right = sort_parallel_single_tree<T, T2>(column, a + (b-a)/2, b, t);
                result = new std::vector<tuple<T>>(left->size() + right->size());
                // TBB in parallel for all available chunks or sequential for each incoming chunk
                std::merge(left->begin(), left->end(), right->begin(), right->end(), result->begin(), t == desc ? tuple_compare_desc<T> : tuple_compare_asc<T>);
                delete(left);
                delete(right);
        }
        return result;
}


template <typename T, typename T2>
std::vector<index123> *sort_parallel_single(std::shared_ptr<arrow::ChunkedArray> column, sort_direction t, sort_type type) {
        std::vector<tuple<T>> *result;
        if (type == tree) {
                result = sort_parallel_single_tree<T, T2>(column, 0, column->num_chunks(), t);
        } else {
                result = sort_parallel_single_flat<T, T2>(column, t);
        }
	std::vector<index123> *r = new std::vector<index123>(result->size());
	// TODO remove this
	for (int i = 0; i < result->size(); i++) {
		(*r)[i] = (*result)[i].index;
	}
	delete(result);
	return r;
}

std::shared_ptr<arrow::Table> sort_dispatch(std::shared_ptr<arrow::Table> table, int column_id, sort_direction t, sort_type type) {
	std::vector<index123> *r;
	auto column = table->column(column_id);
	if (column->type()->id() == arrow::Type::STRING) {
		r = sort_parallel_single<std::string, arrow::StringArray>(column->data(), t, type);
	} else if (column->type()->id() == arrow::Type::INT64) {
		r = sort_parallel_single<arrow::Int64Type::c_type, arrow::Int64Array>(column->data(), t, type);
	} else {
		r = sort_parallel_single<arrow::DoubleType::c_type, arrow::DoubleArray>(column->data(), t, type);
	}
	return sort_finalize(table, r);
}

// Main function
// XXX: there is a bug.. but works in most cases and it is non-goal to fix it now
std::shared_ptr<arrow::Table> sort(std::shared_ptr<arrow::Table> table, std::vector<int> column_ids, std::vector<sort_direction> t, sort_type type) {
	printf("TASK: sorting (%s merge) by %s.\n", type == flat ? "flat" : "tree", column_ids.size() == 1 ? "single column" : "multiple columns");
	auto begin = std::chrono::steady_clock::now();
	std::shared_ptr<arrow::Table> answer;
	if (column_ids.size() == 1) {
		answer = sort_dispatch(table, column_ids[0], t[0], type);
	} else  {
		answer = sort_parallel_multiple(table, column_ids, t, type);
	}
	print_time(begin);
	return answer;
}
#endif

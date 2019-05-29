#ifndef GROUP_BY_H
#define GROUP_BY_H

#include <arrow/api.h>
#include <unordered_map>

#include <print.h>
#include <util.h>

//++++++++++++++++++++++++++++++
// GROUP BY
//++++++++++++++++++++++++++++++

// Multiple columns
struct position {
	int row_index;
	std::vector<std::shared_ptr<arrow::Array>> *arrays;
	bool operator==(const position& other) const {
		for (int i = 0; i < (*arrays).size(); i++) {
			// RangeEquals doesn't work
			//if ((*arrays)[i]->RangeEquals(row_index, row_index+1, other.row_index, (*other.arrays)[i]) == false) {
			if (compare((*arrays)[i], row_index, (*other.arrays)[i], other.row_index) != 0) {
				return false;
			}
		}
		return true;
	}
};

namespace std {
	template <>
	// TODO user defined hash function
	struct hash<position> {
		size_t operator()(const position& p) const { // TODO type
			// Compute individual hash values for two data members and combine them using XOR and bit shifting
			size_t answer = 0;
			for (int i = 0; i < (*p.arrays).size(); i++) { // TODO predefine type somehow
				if ((*p.arrays)[i]->type_id() == arrow::Type::STRING) {
					auto array = std::static_pointer_cast<arrow::StringArray>((*p.arrays)[i]);
					answer ^= hash<std::string>()(array->GetString(p.row_index));
				} else { // TODO for double type
					auto array = std::static_pointer_cast<arrow::Int64Array>((*p.arrays)[i]);
					answer ^= hash<int64_t>()(array->Value(p.row_index));
					//TODO answer ^= ((hash<float>()(k.getM()) ^ (hash<float>()(k.getC()) << 1)) >> 1);
				}
			}
			return answer;
		}
	};
};

struct group {
	std::vector<int> redirection;
	std::vector<std::shared_ptr<arrow::Column>> columns;
	std::vector<std::shared_ptr<arrow::Field>> fields;
};

struct partial_mult_group {
	group *g;
	int temp;
	std::unordered_map<position, int> map;
	std::vector<std::pair<int, int>> fast_build;
};

template <typename T, typename T4>
struct partial_single_group {
	group *g;
	int temp;
	T4 *bld;
	std::unordered_map<T, int> map;
};

void group_by_sequential(std::vector<std::shared_ptr<arrow::Array>> *arrays, partial_mult_group* pg, int n) {
	for (int i = 0; i < (*arrays)[0]->length(); i++) {
		position p{i, arrays}; // TODO copy constructor? move constructor?
		auto number = pg->map.find(p);
		if (number != pg->map.end()) {
			pg->g->redirection.push_back(number->second);
		} else {
			pg->map.insert({p, pg->temp});
			pg->g->redirection.push_back(pg->temp);
			pg->fast_build.push_back({n, i});
			pg->temp++;
		}
	}
}

group* group_by_PARALLEL(std::shared_ptr<arrow::Table> table, std::vector<int> column_ids) {
	printf("      Arrow is columnar database and this request is low performance\n");
	printf("      There are two variants: prebuild caches or not. Executing _without_ prebuilding\n");
	partial_mult_group pg = {new(group), 0};
	// Can different columns have different chunk number? Or it is property of table?
	int num_chunks = table->column(column_ids[0])->data()->num_chunks();
	std::vector<std::vector<std::shared_ptr<arrow::Array>>> all_arrays(num_chunks);
	for (int i = 0; i < num_chunks; i++) {
		for (int j = 0; j < column_ids.size(); j++) {
			all_arrays[i].push_back(table->column(column_ids[j])->data()->chunk(i));
		}
		group_by_sequential(&(all_arrays[i]), &pg, i);
	}

	for (int i = 0; i < column_ids.size(); i++) {
		std::shared_ptr<arrow::ChunkedArray> ca = table->column(column_ids[i])->data();
		std::shared_ptr<arrow::Array> data;
		if (ca->type()->id() == arrow::Type::STRING) {
			arrow::StringBuilder bld;
			for (int j = 0; j < pg.fast_build.size(); j++) {
				bld.Append((std::static_pointer_cast<arrow::StringArray>(ca->chunk(pg.fast_build[j].first)))->GetString(pg.fast_build[j].second));
			}
			bld.Finish(&data);
		} else if (ca->type()->id() == arrow::Type::INT64) {
			arrow::Int64Builder bld;
			for (int j = 0; j < pg.fast_build.size(); j++) {
				bld.Append((std::static_pointer_cast<arrow::Int64Array>(ca->chunk(pg.fast_build[j].first)))->Value(pg.fast_build[j].second));
			}
			bld.Finish(&data);
		} else {
			arrow::DoubleBuilder bld;
			for (int j = 0; j < pg.fast_build.size(); j++) {
				bld.Append((std::static_pointer_cast<arrow::DoubleArray>(ca->chunk(pg.fast_build[j].first)))->Value(pg.fast_build[j].second));
			}
			bld.Finish(&data);
		}

		std::shared_ptr<arrow::Field> field = table->schema()->field(column_ids[i]);
		pg.g->columns.push_back(std::make_shared<arrow::Column>(field->name(), data));
		pg.g->fields.push_back(field);
	}
	return pg.g;
}

// Single column
template <typename T, typename T2, typename T4>
void group_by_sequential(std::shared_ptr<T2> array, partial_single_group<T, T4>* pg) {
	for (int i = 0; i < array->length(); i++) {
		T value = get_value<T, T2>(array, i);
		auto number = pg->map.find(value);
		if (number != pg->map.end()) {
			pg->g->redirection.push_back(number->second);
		} else {
			pg->map.insert({value, pg->temp});
			pg->g->redirection.push_back(pg->temp);
			pg->bld->Append(value);
			pg->temp++;
		}
	}
}

// For group_by by one column
group* group_by_PARALLEL(std::shared_ptr<arrow::Table> table, int column_id) {
	std::shared_ptr<arrow::Field> field = table->schema()->field(column_id);
	std::shared_ptr<arrow::Array> data;
	group* g;
	if (table->column(column_id)->data()->type()->id() == arrow::Type::STRING) {
		arrow::StringBuilder bld;
		partial_single_group<std::string, arrow::StringBuilder> pg = {new(group), 0, &bld};
		for (int i = 0; i < table->column(column_id)->data()->num_chunks(); i++) {
			auto array = std::static_pointer_cast<arrow::StringArray>(table->column(column_id)->data()->chunk(i));
			group_by_sequential<std::string, arrow::StringArray, arrow::StringBuilder>(array, &pg);
		}
		pg.bld->Finish(&data);
		g = pg.g;
	} else { // TODO no double here
		arrow::Int64Builder bld;
		partial_single_group<arrow::Int64Type::c_type, arrow::Int64Builder> pg = {new(group), 0, &bld};
		for (int i = 0; i < table->column(column_id)->data()->num_chunks(); i++) {
			auto array = std::static_pointer_cast<arrow::Int64Array>(table->column(column_id)->data()->chunk(i));
			group_by_sequential<arrow::Int64Type::c_type, arrow::Int64Array, arrow::Int64Builder>(array, &pg);
		}
		pg.bld->Finish(&data);
		g = pg.g;
	}
	g->columns.push_back(std::make_shared<arrow::Column>(field->name(), data));
	g->fields.push_back(field);
	return g;
}

group* group_by(std::shared_ptr<arrow::Table> table, std::vector<int> column_ids) {
	printf("TASK: grouping by %s. (building all group_by columns and NOT counting them)\n", column_ids.size() == 1 ? "single column" : "multiple columns");
	auto begin = std::chrono::steady_clock::now();
	group *g;
	if (column_ids.size() == 1) {
		g = group_by_PARALLEL(table, column_ids[0]);
	} else  {
		g = group_by_PARALLEL(table, column_ids);
	}
	print_time(begin);
	return g;
}

#endif

#ifndef GROUP_BY_H
#define GROUP_BY_H

#define TBB_USE_PERFORMANCE_WARNINGS 1
#define __TBB_STATISTICS 1

#include <arrow/api.h>
#include <unordered_map>

#include <print.h>
#include <util.h>
#include <tbb/tbb.h>
#include <atomic>

//++++++++++++++++++++++++++++++
// GROUP BY
//++++++++++++++++++++++++++++++

// For both single and multiple columns
struct group {
    std::vector<std::vector<int>> redirection;
    std::vector<std::shared_ptr<arrow::Column>> columns;
    std::vector<std::shared_ptr<arrow::Field>> fields;
    std::atomic<int> max_index;
    group(int n = 0): redirection(n), max_index(0) {}
    int get_max_index() { return max_index.load(std::memory_order_relaxed); }
    int increment_index() { return max_index++; }
};

// For multiple columns
struct position {
    int row_index; // TODO like in sort? without array?
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
    operator size_t() const {
        // Compute individual hash values for two data members and combine them using XOR and bit shifting
        size_t answer = 0;
        for (int i = 0; i < arrays->size(); i++) { // TODO predefine type somehow
            if ((*arrays)[i]->type_id() == arrow::Type::STRING) {
                auto array = std::static_pointer_cast<arrow::StringArray>((*arrays)[i]);
                answer ^= std::hash<std::string>()(array->GetString(row_index));
            } else { // TODO for double type
                auto array = std::static_pointer_cast<arrow::Int64Array>((*arrays)[i]);
                answer ^= std::hash<int64_t>()(array->Value(row_index));
                //TODO answer ^= ((hash<float>()(k.getM()) ^ (hash<float>()(k.getC()) << 1)) >> 1);
            }
        }
        return answer;
    }
};

#if 1 //USE_TBB
typedef tbb::concurrent_hash_map<position, int> mult_group_map_t;

void group_by_sequential_multiple( std::vector<std::shared_ptr<arrow::Array>> *arrays
                                 , std::vector<int> &redir, group *g, mult_group_map_t* pg, int n) {
    for (int i = 0, end_i = (*arrays)[0]->length(); i < end_i; i++) {
        position p{i, arrays}; // TODO copy constructor? move constructor?
        mult_group_map_t::accessor a;
        bool uniq = pg->insert(a, p);
        if (!uniq) {
            redir[i] = a->second;
        } else {
            a->second = redir[i] = g->increment_index();
        }
    }
}

#else

namespace std {
template <>
// TODO user defined hash function
struct hash<position> {
    size_t operator()(const position& p) const { // TODO type?
        return size_t(p);
    }
};
}
typedef std::unordered_map<position, int> mult_group_map_t;

void group_by_sequential_multiple( std::vector<std::shared_ptr<arrow::Array>> *arrays
                                 , std::vector<int> &redir, group *g, mult_group_map_t* pg, int n) {
    for (int i = 0, end_i = (*arrays)[0]->length(); i < end_i; i++) {
        position p{i, arrays}; // TODO copy constructor? move constructor?
        auto number = pg->find(p);
        if (number != pg->end()) {
            redir[i] = number->second;
        } else {
            int new_index = g->increment_index();
            redir[i] = new_index;
            pg->insert({p, new_index});
        }
    }
}
#endif

group* group_by_parallel_multiple(std::shared_ptr<arrow::Table> table, std::vector<int> column_ids) {
    printf("      Arrow is columnar database and this request is low performance\n");
    printf("      There are two variants: prebuild hashes or not. Executing _without_ prebuilding\n");
    // Can different columns have different chunk number? Or it is property of table?
    auto *column0 = table->column(column_ids[0])->data().get();
    int num_chunks = column0->num_chunks();

    auto *g = new group{num_chunks};
    std::vector<std::vector<std::shared_ptr<arrow::Array>>> all_arrays(num_chunks);
    mult_group_map_t pg(2048);

    //tbb::parallel_for(0, num_chunks, [](int i) {
    for(int i = 0; i < num_chunks; i++) {
        auto &chunk = all_arrays[i];
        auto &redir = g->redirection[i];
        redir.resize(column0->chunk(i)->length());
        for (int j = 0; j < column_ids.size(); j++) {
            chunk.push_back(table->column(column_ids[j])->data()->chunk(i));
        }
        // TBB in parallel for all available chunks or sequential for each incoming chunk
        group_by_sequential_multiple(&chunk, redir, g, &pg, i);
    }//);

    for (int i = 0; i < column_ids.size(); i++) {
      std::shared_ptr<arrow::ChunkedArray> ca = table->column(column_ids[i])->data();
      std::shared_ptr<arrow::Array> data;
      if (ca->type()->id() == arrow::Type::STRING) {
        std::vector<std::string> new_column(pg.size());
        for (auto j = pg.begin(); j != pg.end(); j++) {
          new_column[j->second] = (std::static_pointer_cast<arrow::StringArray>((*j->first.arrays)[i]))->GetString(j->first.row_index);
        }
        data = vector_to_array<std::string, arrow::StringBuilder>(new_column);
      } else if (ca->type()->id() == arrow::Type::INT64) {
        std::vector<int64_t> new_column(pg.size());
        for (auto j = pg.begin(); j != pg.end(); j++) {
          new_column[j->second] = (std::static_pointer_cast<arrow::Int64Array>((*j->first.arrays)[i]))->Value(j->first.row_index);
        }
        data = vector_to_array<arrow::Int64Type::c_type, arrow::Int64Builder>(new_column);
      } else {
        std::vector<double> new_column(pg.size());
        for (auto j = pg.begin(); j != pg.end(); j++) {
          new_column[j->second] = (std::static_pointer_cast<arrow::DoubleArray>((*j->first.arrays)[i]))->Value(j->first.row_index);
        }
        data = vector_to_array<arrow::DoubleType::c_type, arrow::DoubleBuilder>(new_column);
      }
      std::shared_ptr<arrow::Field> field = table->schema()->field(column_ids[i]);
      g->columns.push_back(std::make_shared<arrow::Column>(field->name(), data));
      g->fields.push_back(field);
    }
    return g;
}

// For single column
template <typename T, typename T4>
struct partial_single_group {
	group *g;
	T4 *bld;
	std::unordered_map<T, int> map;
};

template <typename T, typename T2, typename T4>
void group_by_sequential_single(std::shared_ptr<T2> array, partial_single_group<T, T4>* pg) {
	int s = pg->g->redirection.size();
	pg->g->redirection.push_back(std::vector<int>(0));
	for (int i = 0; i < array->length(); i++) {
		T value = get_value<T, T2>(array, i);
		auto number = pg->map.find(value);
		if (number != pg->map.end()) {
			pg->g->redirection[s].push_back(number->second);
		} else {
			pg->map.insert({value, pg->g->max_index});
			pg->g->redirection[s].push_back(pg->g->max_index);
			pg->bld->Append(value);
			pg->g->max_index++;
		}
	}
}

template <typename T, typename T2, typename T4>
group* group_by_parallel_single(std::shared_ptr<arrow::ChunkedArray> column, std::shared_ptr<arrow::Array>& data) {
    T4 bld;
    partial_single_group<T, T4> pg = {new group(), &bld};
    for (int i = 0; i < column->num_chunks(); i++) {
        auto array = std::static_pointer_cast<T2>(column->chunk(i));
        // TBB in parallel for all available chunks or sequential for each incoming chunk
        group_by_sequential_single<T, T2, T4>(array, &pg);
    }
    pg.bld->Finish(&data);
    return pg.g;
}

group* group_by_dispatch(std::shared_ptr<arrow::Table> table, int column_id) {
    std::shared_ptr<arrow::Field> field = table->schema()->field(column_id);
    std::shared_ptr<arrow::Array> data;
    std::shared_ptr<arrow::ChunkedArray> column = table->column(column_id)->data();
    group* g;
    if (column->type()->id() == arrow::Type::STRING) {
        g = group_by_parallel_single<std::string, arrow::StringArray, arrow::StringBuilder>(column, data);
    } else if (column->type()->id() == arrow::Type::INT64) {
        g = group_by_parallel_single<arrow::Int64Type::c_type, arrow::Int64Array, arrow::Int64Builder>(column, data);
    } else {
        g = group_by_parallel_single<arrow::DoubleType::c_type, arrow::DoubleArray, arrow::DoubleBuilder>(column, data);
    }
    g->columns.push_back(std::make_shared<arrow::Column>(field->name(), data));
    g->fields.push_back(field);
    return g;
}

// Main function
group* group_by(std::shared_ptr<arrow::Table> table, std::vector<int> column_ids) {
    printf("TASK: grouping by %s. (building all group_by columns and NOT counting them)\n", column_ids.size() == 1 ? "single column" : "multiple columns");
    auto begin = std::chrono::steady_clock::now();
    group *g;
    if (column_ids.size() == 1) {
        g = group_by_dispatch(table, column_ids[0]);
    } else  {
        g = group_by_parallel_multiple(table, column_ids);
    }
    print_time(begin);
    return g;
}

#endif

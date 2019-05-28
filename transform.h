#ifndef TRANSFORM_H
#define TRANSFORM_H

#include <arrow/api.h>

#include <print.h>

//++++++++++++++++++++++++++++++
// TRANSFORM
//++++++++++++++++++++++++++++++

// One column tranformation per time. Should aggregation be the same? No vector of tasks?
template <typename T_c, typename T_n, typename T2, typename T4>
std::shared_ptr<arrow::Table> transform(std::shared_ptr<arrow::Table> table, int column_id, T_n (*transformation)(T_c)) {
	printf("TASK: transforming of column\n");
	auto begin = std::chrono::steady_clock::now();

	arrow::ArrayVector new_chunks;
	std::shared_ptr<arrow::Array> data;
	for (int i = 0; i < table->column(column_id)->data()->num_chunks(); i++) {
		T4 bld; // template
		auto c = std::dynamic_pointer_cast<T2>(table->column(column_id)->data()->chunk(i)); // template
		if (c == NULL) {
			printf("Type of %d column is wrong!\n", column_id + 1);
		}
		for (int j = 0; j < c->length(); j++) {
			auto value = c->Value(j);
			T_n new_value = transformation(value);
			bld.Append(new_value); // resize?
		}
		bld.Finish(&data);
		new_chunks.push_back(data);
	}

	std::shared_ptr<arrow::Field> new_field = std::make_shared<arrow::Field>("name", data->type());
	std::shared_ptr<arrow::Column> new_column = std::make_shared<arrow::Column>(new_field, new_chunks);
	std::shared_ptr<arrow::Table> new_table;
	table->SetColumn(column_id, new_column, &new_table);

	print_time(begin);
        return new_table;
}

#endif

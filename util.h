#ifndef UTIL_H
#define UTIL_H

#include <arrow/api.h>

int compare(std::shared_ptr<arrow::Array> a, int ai, std::shared_ptr<arrow::Array> b, int bi) {
	switch (a->type_id()) {
	case arrow::Type::STRING: {
		auto left = std::static_pointer_cast<arrow::StringArray>(a) -> GetString(ai);
		auto right = std::static_pointer_cast<arrow::StringArray>(b) -> GetString(bi);
		if  (left > right) {
			return 1;
		} else if (left == right) {
			return 0;
		}
		return -1;
	}
	case arrow::Type::INT64: {
		auto left = std::static_pointer_cast<arrow::Int64Array>(a) -> Value(ai);
		auto right = std::static_pointer_cast<arrow::Int64Array>(b) -> Value(bi);
		if (left > right) {
			return 1;
		} else if (left == right) {
			return 0;
		}
		return -1;
	}
	case arrow::Type::DOUBLE: {
		auto left = std::static_pointer_cast<arrow::DoubleArray>(a) -> Value(ai);
		auto right = std::static_pointer_cast<arrow::DoubleArray>(b) -> Value(bi);
		if (left > right) {
			return 1;
		} else if (left == right) {
			return 0;
		}
		return -1;
	}
	}
	// TODO VALUES OF OTHER TYPES WILL BE TREATED SAME!!!
	return 0;
}

template <typename T, typename T2>
T get_value(std::shared_ptr<T2> array, int i) {
	if constexpr (std::is_same<T2, arrow::StringArray>::value) {
		return array->GetString(i);
	} else {
		return array->Value(i);
	}
}

template <typename T, typename T4>
std::shared_ptr<arrow::Array> vector_to_array(std::vector<T> values) {
	T4 bld;
	// TODO directly from mutable buffer?
	bld.AppendValues(values); // bld.Append(values[j]) or bld.Resize(values.size()); bld.UnsafeAppend(values[j]);
	std::shared_ptr<arrow::Array> data;
	bld.Finish(&data);
	return data;
}

#endif

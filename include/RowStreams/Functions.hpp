#ifndef ROWSTREAMS_FUNCTIONS_HPP
#define ROWSTREAMS_FUNCTIONS_HPP


#include "RowStreams/Row.hpp"
#include <functional>

namespace RowStreams
{
	/// The combination of two operations, where BinOp can
	/// be a functor compatible with the binary operators in <functional>
	template<class DataType, class Oper1, class Oper2, class BinOp>
	struct BinaryOperator
	{
	public:
		Oper1 oper1_;
		Oper2 oper2_;

		BinaryOperator(const Oper1 & oper1, const Oper2 & oper2)
			: oper1_(oper1), oper2_(oper2)
		{
		}

		DataType operator()(const Row & row) const
		{
			DataType val1 = oper1_(row);
			DataType val2 = oper2_(row);
			return BinOp()(val1, val2);
		}

		void init(const RowDef & rowDef)
		{
			oper1_.init(rowDef);
			oper2_.init(rowDef);
		}
	};

	template<class DataType, class Operator>
	class Function
	{
	public:
		Operator operator_;
		typedef DataType ReturnType;

		Function(const Operator & oper)
			: operator_(oper)
		{
		}

		DataType operator()(const Row & row) const
		{
			return operator_(row);
		}

		void init(const RowDef & rowDef)
		{
			operator_.init(rowDef);
		}

#define FUNCTION_DEF_BIN_OP(op, std_op)\
		template<class Oper2> \
		Function<DataType, BinaryOperator<DataType, Operator, Oper2, std:: std_op <DataType> > > \
			operator op (const Function<DataType, Oper2> & other) \
		{ \
			typedef BinaryOperator<DataType, Operator, Oper2, std:: std_op <DataType> > BinOpType;\
			return Function<DataType, BinOpType >( BinOpType(operator_, other.operator_ ) );\
		}

		FUNCTION_DEF_BIN_OP(+, plus)
		FUNCTION_DEF_BIN_OP(-, minus)
		FUNCTION_DEF_BIN_OP(*, multiplies)

	};

	namespace Functions
	{
		/// Operator that extracts a value from a row column.
		template<class ColumnType>
		class Column
		{
			std::string name_;
			size_t index_;
			size_t offset_;

		public:
			Column(const std::string & name)
				: name_(name), index_(size_t(-1)), offset_(size_t(-1))
			{
			}

			ColumnType operator()(const Row & row) const
			{
				return row.get<ColumnType>(index_, offset_);
			}

			void init(const RowDef & rowDef)
			{
				index_ = rowDef.index(name_);
				offset_ = rowDef.offset(name_);
			}
		};

		template<class ColumnType>
		Function<ColumnType, Column<ColumnType> > column(const std::string & name)
		{
			return Function<ColumnType, Column<ColumnType> >(Column<ColumnType>(name));
		}

		template<class T>
		class Value
		{
			T value_;
		public:
			Value(const T & value)
				: value_(value)
			{
			}

			T operator()(const Row & row) const
			{
				return value_;
			}

			void init(const RowDef & rowDef)
			{
			}
		};

		template<class ValueType>
		Function<ValueType, Value<ValueType> > value(ValueType value)
		{
			return Function<ValueType, Value<ValueType> >(Value<ValueType>(value));
		}

	}
}

#endif
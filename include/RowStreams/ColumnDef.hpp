#ifndef ROWSTREAMS_COLUMN_DEF_HPP
#define ROWSTREAMS_COLUMN_DEF_HPP

#include <string>

namespace RowStreams
{
	class Row;
	/// Information about the data type of a column, as well as utilities
	/// to perform operations on the column value.
	class ColumnDef
	{
		std::string name_;
		size_t index_;
		size_t offset_;

	public:
		ColumnDef(const std::string & name)
			: name_(name), index_(size_t(-1)), offset_(size_t(-1))
		{
		}

		virtual ~ColumnDef(){}

		virtual void parseString(const char * value, Row & row) const = 0;
		virtual std::string toString(Row & row) const = 0;
		virtual size_t size() const = 0;
		virtual size_t alignment() const = 0;
		virtual ColumnDef * clone() const = 0;


		std::string name()
		{
			return name_;
		}

		size_t offset() const
		{
			return offset_;
		}

		void offset(size_t new_offset)
		{
			offset_ = new_offset;
		}

		size_t index() const
		{
			return index_;
		}

		void index(size_t new_index)
		{
			index_ = new_index;
		}
	};


}

#endif
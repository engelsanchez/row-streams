#ifndef ROWSTREAMS_COLUMN_DEF_HELPERS_HPP
#define ROWSTREAMS_COLUMN_DEF_HELPERS_HPP

#include "RowStreams/Row.hpp"
#include "RowStreams/ValueParser.hpp"
#include <sstream>
#include <boost/type_traits.hpp>

namespace RowStreams
{
	template< class T>
	class ColumnDefTpl : public ColumnDef
	{
	public:

		ColumnDefTpl(const std::string & name)
			: ColumnDef(name)
		{
		}

		void parseString(const char * str, Row & row) const
		{
			ValueParser<T> parser;
			T value = parser(str);
			row.set(index(), offset(), value);
		}

		std::string toString(Row & row) const
		{
			std::ostringstream oss;
			if(!row.isNull(index()))
				oss << row.get<T>(index(), offset());
			return oss.str();
		}

		size_t size() const
		{
			return sizeof(T);
		}

		size_t alignment() const
		{
			return boost::alignment_of<T>::value;
		}

		ColumnDef * clone() const
		{
			return new ColumnDefTpl<T>(*this);
		}
	};

	template<class T>
	ColumnDefTpl<T> col_def(const std::string & name)
	{
		return ColumnDefTpl<T>(name);
	}

	bool same_name(ColumnDef * attr1, ColumnDef * attr2)
	{
		return attr1->name() == attr2->name();
	}




}

#endif
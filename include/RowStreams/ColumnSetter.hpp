#ifndef ROWSTREAMS_COLUMN_SETTER_HPP
#define ROWSTREAMS_COLUMN_SETTER_HPP

#include "RowStreams/RowDef.hpp"
#include "RowStreams/Functions.hpp"
#include <string>

namespace RowStreams
{
	template<class Source, class ColumnType, class Oper>
	class ColumnSetter
	{
		Source * source_;
		std::string name_;
		Function<ColumnType, Oper> function_;
		RowDef rowDef_;
		size_t index_;
		size_t offset_;

	public:
		ColumnSetter(const std::string & name, const Function<ColumnType, Oper> & function)
			: source_(0), name_(name), function_(function), index_(size_t(-1)), offset_(size_t(-1))
		{
		}

		Row * next() const
		{
			Row * row = source_->next();
			if(row)
				row->set(index_, offset_, function_(*row));

			return row;
		}

		void init()
		{
			source_->init();
			rowDef_ = source_->rowDef();
			index_ = rowDef_.index(name_);
			offset_ = rowDef_.offset(name_);
			function_.init(rowDef_);
		}

		const RowDef & rowDef()
		{
			return rowDef_;
		}

		void source(Source * source)
		{
			source_ = source;
		}
	};

	template<class ColumnType, class Oper>
	class ColumnSetterPrototype
	{
		std::string name_;
		Function<ColumnType, Oper> function_;
	public:

	    template<class Source>
		struct ForSource
		{
			typedef ColumnSetter<Source, ColumnType, Oper> Type;
		};

		ColumnSetterPrototype(const std::string & name, const Function<ColumnType, Oper> & function )
			: name_(name), function_(function)
		{
		}

		template<class Source>
		ColumnSetter<Source, ColumnType, Oper>
			create() const
		{
			return ColumnSetter<Source, ColumnType, Oper>(name_, function_);
		}
	};

	template<class ColumnType, class Oper>
	ColumnSetterPrototype<ColumnType, Oper>
		set_column(const std::string & name, const Function<ColumnType, Oper> & func)
	{
		return ColumnSetterPrototype<ColumnType, Oper>(name, func);
	}
}

#endif
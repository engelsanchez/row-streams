#ifndef ROWSTREAMS_COLUMN_ADDER_HPP
#define ROWSTREAMS_COLUMN_ADDER_HPP

#include <string>

namespace RowStreams
{
	template<class Source, class ColumnType>
	class ColumnAdder
	{
		/// Row source. We don't own it, so no deletes.
		Source * source_;
		std::string name_;
		RowDef rowDef_;

	public:
		ColumnAdder(const std::string & name)
			: source_(0), name_(name)
		{
		}

		void init()
		{
			source_->init();
			rowDef_ = source_->rowDef() << column_def<ColumnType>(name_);
		}

		void source(Source * source)
		{
			source_ = source;
		}


		Row * next()
		{
			Row * row = source_->next();
			if(row)
				row->rowDef(&rowDef_);

			return row;
		}

		const RowDef & rowDef() const
		{
			return rowDef_;
		}
	};

	template<class ColumnType>
	class ColumnAdderPrototype
	{
		std::string name_;
	public:
		template<class Source>
		struct ForSource
		{
			typedef ColumnAdder<Source, ColumnType> Type;
		};

		ColumnAdderPrototype(const std::string & name)
			: name_(name)
		{
		}

		template<class Source>
		ColumnAdder<Source, ColumnType>
			create() const
		{
			return ColumnAdder<Source, ColumnType>(name_);
		}
	};

	template<class ColumnType>
	ColumnAdderPrototype<ColumnType>
		add_column(const std::string & name)
	{
		return ColumnAdderPrototype<ColumnType>(name);
	}
}

#endif
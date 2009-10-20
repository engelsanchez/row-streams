#ifndef ROWSTREAMS_ROW_DEF_HPP
#define ROWSTREAMS_ROW_DEF_HPP

#include <vector>
#include <map>
#include "RowStreams/ColumnDef.hpp"

namespace RowStreams
{
	class RowDef
	{
	public:
		typedef std::vector<ColumnDef*> ColumnDefVector;
		typedef ColumnDefVector::const_iterator ConstAttrIter;

	private:
		ColumnDefVector columnDefs_;
		typedef std::map<std::string, ColumnDef*> AttrMap;
		AttrMap attrMap_;

		size_t size_;
		/// XXX Look for better way to guarantee safe alignment
		enum { MIN_SIZE = 8 };

		/// Does not allow assignment operation.
		RowDef & operator=(const RowDef & other);

	public:
		RowDef()
			: size_(0)
		{
		}

		RowDef(const RowDef & other)
			:size_(other.size_)
		{
			for(ConstAttrIter col_iter = other.begin();
				col_iter != other.end();
				++col_iter)
			{
				columnDefs_.push_back((*col_iter)->clone());
			}

			for(ConstAttrIter col_iter = begin();
				col_iter != end();
				++col_iter)
			{
				ColumnDef * col = *col_iter;
				attrMap_[col->name()] = col;
			}
		}


		~RowDef()
		{
			for(ConstAttrIter attr = begin(); attr != end(); ++attr)
			{
				delete *attr;
			}
		}

		void add(const ColumnDef & columnDef)
		{
			ColumnDef * columnDefCopy = columnDef.clone();
			size_t ofs = size_ + size_ % columnDefCopy->alignment();
			columnDefCopy->offset(ofs);
			columnDefCopy->index(columnDefs_.size());
			size_ = ofs + columnDefCopy->size();

			columnDefs_.push_back(columnDefCopy);
		}

		/// Sugar baby, yeah!
		RowDef & operator << (const ColumnDef & columnDef)
		{
			add(columnDef);
			return *this;
		}

		size_t size() const
		{
			return std::max(size_, size_t(MIN_SIZE));
		}

		/// Returns a newly created buffer to be used by a Row object 
		/// that follows this definition.
		char * newBuffer() const
		{
			return new char[size()];
		}

		ConstAttrIter begin() const
		{
			return columnDefs_.begin();
		}

		ConstAttrIter end() const
		{
			return columnDefs_.end();
		}

		size_t numColumns() const
		{
			return columnDefs_.size();
		}

		const ColumnDef * columnDef(const std::string & name) const
		{
			AttrMap::const_iterator it = attrMap_.find(name);
			if(it != attrMap_.end())
				return it->second;
			return 0;
		}

		size_t offset(const std::string & name) const
		{
			AttrMap::const_iterator it = attrMap_.find(name);
			if(it != attrMap_.end())
				return it->second->offset();

			throw std::runtime_error("No offset for invalid column "+name);
		}

		size_t offset(size_t index) const
		{
			if(index >= columnDefs_.size())
				throw new std::runtime_error("Invalid column index");

			return columnDefs_[index]->offset();
		}
	};

}

#endif
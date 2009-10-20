#ifndef ROWSTREAMS_ROW_DEF_HPP
#define ROWSTREAMS_ROW_DEF_HPP

#include <vector>
#include <map>
#include "RowStreams/ColumnDef.hpp"

namespace RowStreams
{
	/// Holds information about the layout of the buffer used by rows in a stream,
	/// including names, indices and offsets for all columns and buffer capacity,
	/// which may be bigger than its size to account for rows that grow as they go
	/// down the stream.
	class RowDef
	{
	public:
		typedef std::vector<ColumnDef*> ColumnDefVector;
		typedef ColumnDefVector::const_iterator ConstAttrIter;

	private:
		/// A minimum size will tell the new operator to allocate
		/// chunks correctly aligned for any data this size or smaller.
		/// XXX Look for more portable way to guarantee safe alignment and use less data per row.
		enum { MIN_SIZE = 8 };
		size_t size_;
		size_t capacity_;

		ColumnDefVector columnDefs_;
		typedef std::map<std::string, ColumnDef*> AttrMap;
		AttrMap attrMap_;

	public:
		RowDef()
			: size_(0), capacity_(0)
		{
		}

		RowDef(const RowDef & other)
			: size_(other.size_), capacity_(other.capacity_)
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

		RowDef & operator=(const RowDef & other)
		{
			size_ = other.size_;
			capacity_ = other.capacity_;

			for(ConstAttrIter attr = begin(); attr != end(); ++attr)
			{
				delete *attr;
			}

			columnDefs_.clear();
			attrMap_.clear();

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

			return *this;
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

		RowDef operator << (const ColumnDef & columnDef) const
		{
			RowDef rowDefCopy = *this;
			rowDefCopy.add(columnDef);
			return rowDefCopy;
		}

		size_t size() const
		{
			return std::max(size_, size_t(MIN_SIZE));
		}

		size_t capacity() const
		{
			return std::max(size(), capacity_);
		}

		void capacity(size_t capacity)
		{
			capacity_ = capacity;
		}

		/// Returns a newly created buffer to be used by a Row object 
		/// that follows this definition.
		char * newBuffer() const
		{
			return new char[capacity()];
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

		size_t index(const std::string & name) const
		{
			AttrMap::const_iterator it = attrMap_.find(name);
			if(it != attrMap_.end())
				return it->second->index();

			throw std::runtime_error("No index for invalid column "+name);
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
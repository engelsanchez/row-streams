#ifndef ROWSTREAMS_ROW_HPP
#define ROWSTREAMS_ROW_HPP

#include "RowStreams/RowDef.hpp"
#include <vector>
#include <cstring>

namespace RowStreams
{
	/// Represents a row of data containing a number of columns 
	/// of potentially different plain data types.
	class Row
	{
		const RowDef * rowDef_;
		char * buf_;
		/// A bit for each column value in the row, where zero means
		/// that column is null.
		std::vector<bool> valueSet_;

	public:
		Row(const RowDef * rowDef)
			: rowDef_(rowDef), 
			buf_(rowDef_->newBuffer())
		{
			valueSet_.assign(rowDef_->numColumns(), false);
		}

		template<class T>
		T get(size_t index, size_t ofs) const
		{
			return *((T*)(buf_+ofs));
		}

		template<class T>
		T get(size_t index) const
		{
			size_t ofs = rowDef_->offset(index);
			return get(index, ofs);
		}

		bool isNull(size_t index) const
		{
			return !valueSet_[index];
		}

		template<class T>
		void set(size_t index, size_t ofs, T value)
		{
			*((T*)(buf_+ofs)) = value;
			valueSet_[index] = true;
		}

		template<class T>
		void set(size_t index, T value)
		{
			size_t ofs = rowDef_->offset(index);
			set(index, ofs, value);
		}

		const RowDef * rowDef() const
		{
			return rowDef_;
		}

		void rowDef(const RowDef * rowDef)
		{
			size_t new_capacity = rowDef->capacity();
			size_t old_capacity = rowDef_->capacity();

			if(new_capacity = old_capacity)
			{
				char * new_buf = new char[new_capacity];
				::memcpy(new_buf, buf_, std::min(rowDef_->size(), rowDef->size()));
				delete buf_;
				buf_ = new_buf;
			}
			rowDef_ = rowDef;
		}

	};

}

#endif
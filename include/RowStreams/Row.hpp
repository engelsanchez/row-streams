#ifndef ROWSTREAMS_ROW_HPP
#define ROWSTREAMS_ROW_HPP

#include "RowStreams/RowDef.hpp"
#include <vector>

namespace RowStreams
{
	/// Represents a row of data containing a number of columns 
	/// of potentially different data types.
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
		T get(size_t index, size_t ofs)
		{
			return *((T*)(buf_+ofs));
		}

		template<class T>
		T get(size_t index)
		{
			size_t ofs = rowDef_->offset(index);
			return get(index, ofs);
		}

		bool isNull(size_t index)
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
	};

}

#endif
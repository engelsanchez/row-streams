#ifndef ROWSTREAMS_TEXT_FLAT_FILE_READER_HPP
#define ROWSTREAMS_TEXT_FLAT_FILE_READER_HPP

#include "RowStreams/Row.hpp"
#include "RowStreams/ColumnDef.hpp"
#include <vector>
#include <string>
#include <fstream>
#include <algorithm>

namespace RowStreams
{
	/// Reads rows from a text file containing newline delimited rows of tab
	/// (or other configurable character) delimited columns.
	class TextFlatFileReader
	{
		RowDef         rowDef_;
		std::string    fileName_;
		char           sep_;
		std::ifstream  ifs_;
		std::string    line_;

		typedef std::vector<const ColumnDef*> ColAttrs;
		ColAttrs       colAttrs_;

	public:
		TextFlatFileReader(const RowDef & rowDef, const std::string & file_name, const char sep = '\t')
			: rowDef_(rowDef), fileName_(file_name), sep_(sep), ifs_(fileName_.c_str())
		{
			if(ifs_.fail())
				throw std::runtime_error("Failed to open "+fileName_);
			// read header
			std::getline(ifs_, line_);
			size_t pos1 = 0;

			size_t pos2;
			do {
				pos2 = line_.find_first_of(sep_, pos1);
				std::string name = line_.substr(pos1, pos2);
				pos1 = pos2+1;
				colAttrs_.push_back(rowDef_.columnDef(name));
			}while(pos2 != std::string::npos);
		}

		Row * next()
		{
			if(!ifs_.good())
				return 0;

			std::getline(ifs_, line_);
			// Although potentially ambiguous, ignoring single carriage return in last line of file.
			if(!ifs_.good() && line_.empty())
				return 0;

			line_.append(1, '\0');
			std::replace(line_.begin(), line_.end(), sep_, '\0');

			ColAttrs::const_iterator col_attr = colAttrs_.begin();
			const ColAttrs::const_iterator col_attr_end = colAttrs_.end();

			Row * row = new Row(&rowDef_);

			const char * field_str = line_.c_str();
			for(std::string::size_type sep_pos = line_.find_first_of('\0');
				sep_pos != std::string::npos;
				++sep_pos, field_str = line_.c_str() + sep_pos, sep_pos = line_.find_first_of('\0', sep_pos), ++col_attr)
			{
				const ColumnDef * columnDef = *col_attr;

				if(!columnDef)
					continue;

				columnDef->parseString(field_str, *row);
			}

			return row;
		}

		const RowDef & rowDef()
		{
			return rowDef_;
		}
	};

}

#endif
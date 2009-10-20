#ifndef ROWSTREAMS_TEXT_FLAT_FILE_WRITER_HPP
#define ROWSTREAMS_TEXT_FLAT_FILE_WRITER_HPP

#include <string>
#include <fstream>
#include <stdexcept>

namespace RowStreams
{
	/// Stores row data in a text file.
	/// Tab characters are output in between columns and
	/// newline characters in between rows.
	template<class Source>
	class TextFlatFileWriter
	{
		Source * source_;
		std::string fileName_;
		char colSep_;
		char rowSep_;

	public:
		TextFlatFileWriter(Source * source, const std::string & fileName)
			: source_(source), fileName_(fileName), colSep_('\t'), rowSep_('\n')
		{
		}

		void run()
		{
			std::ofstream ofs(fileName_.c_str());
			if(!ofs)
				throw new std::runtime_error("Could not open file "+fileName_);

			const RowDef & rowDef = source_->rowDef();

			bool first = true;
			for(RowDef::ConstAttrIter col_iter = rowDef.begin();
				col_iter != rowDef.end();
				++col_iter)
			{
				if(!first)
				{
					ofs << colSep_;
				}
				else
				{
					first = false;
				}

				ColumnDef * col = *col_iter;
				ofs << col->name();
			}
			ofs << rowSep_;
			
			while(Row * row = source_->next())
			{
				bool first = true;
				for(RowDef::ConstAttrIter col_iter = rowDef.begin();
					col_iter != rowDef.end();
					++col_iter)
				{
					if(!first)
					{
						ofs << colSep_;
					}
					else
					{
						first = false;
					}

					ColumnDef * col = *col_iter;
					ofs << col->toString(*row);
				}
				ofs << rowSep_;
			}

		}
	};

}

#endif
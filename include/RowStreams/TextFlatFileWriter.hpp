#ifndef ROWSTREAMS_TEXT_FLAT_FILE_WRITER_HPP
#define ROWSTREAMS_TEXT_FLAT_FILE_WRITER_HPP

#include "RowStreams/RowDef.hpp"
#include <string>
#include <fstream>
#include <stdexcept>

namespace RowStreams
{
	/// Stores a row stream in a text file.
	/// By default uses tab characters are in between columns and
	/// newline characters in between rows.
	template<class Source>
	class TextFlatFileWriter
	{
		/// Row source. We don't own it, so no deletes.
		Source * source_;
		std::string fileName_;
		char colSep_;
		char rowSep_;
		std::ofstream ofs_;
		RowDef rowDef_;

	public:
		TextFlatFileWriter(const std::string & fileName)
			: source_(0), fileName_(fileName), colSep_('\t'), rowSep_('\n')
		{
		}

		TextFlatFileWriter(const TextFlatFileWriter & other)
			: source_(0), fileName_(other.fileName_), colSep_(other.colSep_), rowSep_(other.rowSep_)
		{
		}

		TextFlatFileWriter & operator=(const TextFlatFileWriter & other)
		{
			if(ofs_.open())
				ofs_.close();
			source_ = 0;
			fileName_ = other.fileName_;
			colSep_ = other.colSep_;
			rowSep_ = other.rowSep_;
		}

		void source(Source * source)
		{
			source_ = source;
		}

		void init()
		{
			source_->init();

			ofs_.open(fileName_.c_str());
			if(!ofs_)
				throw new std::runtime_error("Could not open file "+fileName_);

			rowDef_ = source_->rowDef();
		}

		void run()
		{

			bool first = true;
			for(RowDef::ConstAttrIter col_iter = rowDef_.begin();
				col_iter != rowDef_.end();
				++col_iter)
			{
				if(!first)
				{
					ofs_ << colSep_;
				}
				else
				{
					first = false;
				}

				ColumnDef * col = *col_iter;
				ofs_ << col->name();
			}
			ofs_ << rowSep_;
			
			while(Row * row = source_->next())
			{
				bool first = true;
				for(RowDef::ConstAttrIter col_iter = rowDef_.begin();
					col_iter != rowDef_.end();
					++col_iter)
				{
					if(!first)
					{
						ofs_ << colSep_;
					}
					else
					{
						first = false;
					}

					ColumnDef * col = *col_iter;
					ofs_ << col->toString(*row);
				}
				ofs_ << rowSep_;
			}

		}
	};

	/// Bridge class used in the pipeline construction syntax.
	class TextFlatFileWriterPrototype
	{
		std::string fileName_;
	public:

		template<class Source>
		struct ForSource
		{
			typedef TextFlatFileWriter<Source> Type;
		};
		
		TextFlatFileWriterPrototype(const std::string & fileName)
			: fileName_(fileName)
		{
		}

		template<class Source>
		TextFlatFileWriter<Source> create() const
		{
			return TextFlatFileWriter<Source>(fileName_);
		}
	};

	TextFlatFileWriterPrototype write_text_file(const std::string & fileName)
	{
		return TextFlatFileWriterPrototype(fileName);
	}

} // end namespace RowStreams

#endif
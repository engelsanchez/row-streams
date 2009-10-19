#include <iostream>
#include <cstring>
#include <fstream>
#include <string>
#include <vector>
#include <map>
#include <bitset>
#include <algorithm>
#include <cerrno>
#include <boost/type_traits.hpp>

namespace RowStreams
{
	class Row;

	template<class T>
	class ValueParser
	{
	public:
		T operator()(const char * str)
		{
			std::istringstream is(str);
			T tmp;
			is >> tmp;
			return tmp;
		}
	};

	template<>
	class ValueParser<int>
	{
	public:
		int operator()(const char * str)
		{
			errno = 0;
			int tmp = ::atoi(str);
			if(errno != 0)
				std::clog << "Error parsing " << str << " as an integer value" << std::endl;
			return tmp;
		}
	};

	template<>
	class ValueParser<double>
	{
	public:
		double operator()(const char * str)
		{
			errno = 0;
			double tmp = ::atof(str);
			if(errno != 0)
				std::clog << "Error parsing " << str << " as a double value" << std::endl;
			return tmp;
		}
	};

	/// Information about the data type of a column, as well as utilities
	/// to perform operations on the column value.
	class AttrDef
	{
		std::string name_;
		size_t index_;
		size_t offset_;

	public:
		AttrDef(const std::string & name)
			: name_(name), index_(size_t(-1)), offset_(size_t(-1))
		{
		}

		virtual ~AttrDef(){}

		virtual void parseString(const char * value, Row & row) const = 0;
		virtual std::string toString(Row & row) const = 0;
		virtual size_t size() const = 0;
		virtual size_t alignment() const = 0;
		virtual AttrDef * clone() const = 0;


		std::string name()
		{
			return name_;
		}

		size_t offset() const
		{
			return offset_;
		}

		void offset(size_t new_offset)
		{
			offset_ = new_offset;
		}

		size_t index() const
		{
			return index_;
		}

		void index(size_t new_index)
		{
			index_ = new_index;
		}
	};

	template< class T>
	class AttrDefTpl : public AttrDef
	{
	public:

		AttrDefTpl(const std::string & name)
			: AttrDef(name)
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

		AttrDef * clone() const
		{
			return new AttrDefTpl<T>(*this);
		}
	};

	template<class T>
	AttrDefTpl<T> col_def(const std::string & name)
	{
		return AttrDefTpl<T>(name);
	}

	class RowDef
	{
	public:
		typedef std::vector<AttrDef*> AttrDefVector;
		typedef AttrDefVector::const_iterator ConstAttrIter;

	private:
		AttrDefVector attrDefs_;
		typedef std::map<std::string, AttrDef*> AttrMap;
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
				attrDefs_.push_back((*col_iter)->clone());
			}

			for(ConstAttrIter col_iter = begin();
				col_iter != end();
				++col_iter)
			{
				AttrDef * col = *col_iter;
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

		void add(const AttrDef & attrDef)
		{
			AttrDef * attrDefCopy = attrDef.clone();
			size_t ofs = size_ + size_ % attrDefCopy->alignment();
			attrDefCopy->offset(ofs);
			attrDefCopy->index(attrDefs_.size());
			size_ = ofs + attrDefCopy->size();

			attrDefs_.push_back(attrDefCopy);
		}

		char * newBuffer() const
		{
			return new char[std::max(size_, size_t(MIN_SIZE))];
		}

		ConstAttrIter begin() const
		{
			return attrDefs_.begin();
		}

		ConstAttrIter end() const
		{
			return attrDefs_.end();
		}

		size_t numColumns() const
		{
			return attrDefs_.size();
		}

		const AttrDef * attrDef(const std::string & name) const
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
			if(index >= attrDefs_.size())
				throw new std::runtime_error("Invalid column index");

			return attrDefs_[index]->offset();
		}
	};

	/// Represents a row of data containing a number of columns 
	/// of potentially different data types.
	class Row
	{
		const RowDef * rowDef_;
		char * buf_;
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

	bool same_name(AttrDef * attr1, AttrDef * attr2)
	{
		return attr1->name() == attr2->name();
	}

	/// Reads rows from a text file containing newline delimited rows of tab
	/// (or other configurable character) delimited columns.
	class TextFlatFileReader
	{
		RowDef         rowDef_;
		std::string    fileName_;
		char           sep_;
		std::ifstream  ifs_;
		std::string    line_;

		typedef std::vector<const AttrDef*> ColAttrs;
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
				colAttrs_.push_back(rowDef_.attrDef(name));
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
				const AttrDef * attrDef = *col_attr;

				if(!attrDef)
					continue;

				attrDef->parseString(field_str, *row);
			}

			return row;
		}

		const RowDef & rowDef()
		{
			return rowDef_;
		}
	};

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

				AttrDef * col = *col_iter;
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

					AttrDef * col = *col_iter;
					ofs << col->toString(*row);
				}
				ofs << rowSep_;
			}

		}
	};

} // end namespace RowStreams

using namespace RowStreams;

int main(int argc, char ** argv)
{
	RowDef rowDef;
	rowDef.add(col_def<int>("a"));
	rowDef.add(col_def<double>("b"));
	TextFlatFileReader reader(rowDef, "input.txt");

	TextFlatFileWriter<TextFlatFileReader> writer(&reader, "output.txt");

	writer.run();
	return 0;
}
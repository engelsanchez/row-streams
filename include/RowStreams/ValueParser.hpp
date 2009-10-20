#ifndef ROWSTREAMS_VALUE_PARSER_HPP
#define ROWSTREAMS_VALUE_PARSER_HPP

#include <iostream>

namespace RowStreams
{
	/// Parses a value from a c-string.
	/// TODO: Suitable for numeric types, but not for string values yet.
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

	/// ValueParser specialization for ints. Notice that
	/// the default template can handle ints, but specialization
	/// will be useful to allow for faster, custom made parsers.
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

	/// ValueParser specialization for doubles. Notice that
	/// the default template can handle doubles, but specialization
	/// will be useful to allow for faster, custom made parsers.
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

}


#endif
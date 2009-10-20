
#include "RowStreams.hpp"


using namespace RowStreams;
using namespace RowStreams::Functions;

int main(int argc, char ** argv)
{
	try 
	{
		Pipeline p (
			read_text_file( RowDef() << column_def<int>("a") << column_def<double>("b"), "test/input.txt")
			>> add_column<double>("c")
			>> set_column("c", column<double>("b") * value(2.0) + value(3.0))
			>> write_text_file("test/output.txt") 
			);

		p.run();
	} 
	catch(std::runtime_error & e)
	{
		std::cerr << e.what() << std::endl;
		return 1;
	}
	catch(...)
	{
		std::cerr << "Unknown exception caught" << std::endl;
		return 1;
	}
	return 0;
}
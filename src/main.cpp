
#include "RowStreams.hpp"

using namespace RowStreams;

int main(int argc, char ** argv)
{
	TextFlatFileReader reader(
		RowDef() << col_def<int>("a") << col_def<double>("b"), 
		"input.txt");

	TextFlatFileWriter<TextFlatFileReader> writer(&reader, "output.txt");

	writer.run();
	return 0;
}
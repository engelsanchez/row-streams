#ifndef ROWSTREAMS_PIPELINE_HPP
#define ROWSTREAMS_PIPELINE_HPP

namespace RowStreams
{
	class NoModule {};

	template<class Module, class PrevModule = NoModule>
	class PartialPipeline
	{
		PrevModule prev_;
		Module module_;
	public:
		typedef PartialPipeline<Module, PrevModule> MyType;

		template<class T>
		class ForSource
		{
			typedef Module Type;
		};

		PartialPipeline(const PrevModule & prev, const Module & module)
			: prev_(prev), module_(module)
		{
		}

		void init()
		{
			module_.source(&prev_);
			module_.init();
		}

		Row * next()
		{
			return module_.next();
		}

		const RowDef & rowDef()
		{
			return module_.rowDef();
		}

		void run()
		{
			module_.run();
		}

		template<class Prototype>
		PartialPipeline<typename Prototype::ForSource<MyType>::Type, MyType>  
			operator >> ( const Prototype & prototype) const
		{
			return PartialPipeline<typename Prototype::ForSource<MyType>::Type, MyType >(*this, prototype.create<MyType>());
		}
	};

	class Pipeline
	{
		class Runnable
		{
		public:
			virtual ~Runnable(){}
			virtual void run() = 0;
		} * runnable_;

		template<class T>
		class RunnableWrapper : public Runnable
		{
			T wrapped_;
		public:
			RunnableWrapper(const T & wrapped)
				: wrapped_(wrapped)
			{
			}

			~RunnableWrapper()
			{
			}

			void run()
			{
				wrapped_.init();
				wrapped_.run();
			}
		};



	public:
		Pipeline()
			: runnable_(0)
		{
		}

		template<class Module, class PrevModule>
		explicit Pipeline(const PartialPipeline<Module, PrevModule> & t)
			: runnable_( new RunnableWrapper< PartialPipeline<Module, PrevModule> >(t) )
		{
		}

		~Pipeline()
		{
			delete runnable_;
		}


		void run()
		{
			if(runnable_)
				runnable_->run();
		}
	};


}

#endif
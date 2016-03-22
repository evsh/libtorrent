/*

Copyright (c) 2007-2016, Arvid Norberg
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions
are met:

    * Redistributions of source code must retain the above copyright
      notice, this list of conditions and the following disclaimer.
    * Redistributions in binary form must reproduce the above copyright
      notice, this list of conditions and the following disclaimer in
      the documentation and/or other materials provided with the distribution.
    * Neither the name of the author nor the names of its
      contributors may be used to endorse or promote products derived
      from this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
POSSIBILITY OF SUCH DAMAGE.

*/

#ifndef TORRENT_DISK_BUFFER_POOL_HPP
#define TORRENT_DISK_BUFFER_POOL_HPP

#include "libtorrent/config.hpp"

#include "libtorrent/aux_/disable_warnings_push.hpp"

#include <vector>
#include <boost/shared_ptr.hpp>
#include <boost/function.hpp>
#include <boost/utility.hpp>

#include "libtorrent/aux_/disable_warnings_pop.hpp"

#include "libtorrent/thread.hpp"
#include "libtorrent/bitfield.hpp"
#include "libtorrent/io_service_fwd.hpp"
#include "libtorrent/file.hpp" // for iovec_t

namespace libtorrent
{
	namespace aux { struct session_settings; }
	class alert;
	struct disk_observer;

	struct TORRENT_EXTRA_EXPORT disk_buffer_pool : boost::noncopyable
	{
		disk_buffer_pool(int block_size, io_service& ios
			, boost::function<void()> const& trigger_trim);
		~disk_buffer_pool();

#if TORRENT_USE_ASSERTS
		bool is_disk_buffer(char* buffer
			, mutex::scoped_lock& l) const;
		bool is_disk_buffer(char* buffer) const;
#endif

		char* allocate_buffer(char const* category);
		char* allocate_buffer(bool& exceeded, boost::shared_ptr<disk_observer> o
			, char const* category);
		void free_buffer(char* buf);
		void free_multiple_buffers(char** bufvec, int numbufs);

		int allocate_iovec(file::iovec_t* iov, int iov_len);
		void free_iovec(file::iovec_t* iov, int iov_len);

		int block_size() const { return m_block_size; }

		void release_memory();

		boost::uint32_t in_use() const
		{
			mutex::scoped_lock l(m_pool_mutex);
			return m_in_use;
		}
		boost::uint32_t num_to_evict(int num_needed = 0);
		bool exceeded_max_size() const { return m_exceeded_max_size; }

		void set_settings(aux::session_settings const& sett, error_code& ec);

	protected:

		void free_buffer_impl(char* buf, mutex::scoped_lock& l);
		char* allocate_buffer_impl(mutex::scoped_lock& l, char const* category);

		void allocate_pool(error_code& ec);
		void free_pool();

		// number of bytes per block. The BitTorrent
		// protocol defines the block size to 16 KiB.
		const int m_block_size;

		// number of disk buffers currently allocated
		int m_in_use;

		// cache size limit
		int m_max_use;

		// if we have exceeded the limit, we won't notify peers of allowing
		// allocations again until we drop below this low watermark
		int m_low_watermark;

		// if we exceed this limit, we start telling peers we're full and that
		// they should wait for notifications of being able to allocate new blocks
		int m_high_watermark;

		// if we exceed the max number of buffers, we start
		// adding up callbacks to this queue. Once the number
		// of buffers in use drops below the low watermark,
		// we start calling these functions back
		std::vector<boost::weak_ptr<disk_observer> > m_observers;

		// callback used to tell the cache it needs to free up some blocks
		boost::function<void()> m_trigger_cache_trim;

		// set to true to throttle more allocations
		bool m_exceeded_max_size;

		// this is the main thread io_service. Callbacks are
		// posted on this in order to have them execute in
		// the main thread.
		io_service& m_ios;

	private:

		void check_buffer_level(mutex::scoped_lock& l);

		mutable mutex m_pool_mutex;

		// the pointer to the block of virtual address space
		// making up the mmapped cache space
		char* m_cache_pool;

		// the actual size, in bytes, of the memory block at m_cache_pool
		boost::int64_t m_pool_size;

		// TODO: 2 this could probably be optimized by keeping an index to the first
		// known free block. Every time a block is freed with a lower index, it's
		// updated. When that block is allocated, the cursor is cleared.

		// each block in the cache_pool has a corresponding bit in this bitfield.
		// a set bit means the block is free to be used, a cleared bit means
		// the block is in use.
		bitfield m_free_blocks;

		// this is specifically exempt from release_asserts
		// since it's a quite costly check. Only for debug
		// builds.
#if TORRENT_USE_ASSERTS
		bitfield m_buffers_in_use;
		int m_magic;
		bool m_settings_set;
#endif
	};

}

#endif // TORRENT_DISK_BUFFER_POOL_HPP


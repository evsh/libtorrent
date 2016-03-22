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

#include "libtorrent/config.hpp"
#include "libtorrent/disk_buffer_pool.hpp"
#include "libtorrent/assert.hpp"
#include "libtorrent/allocator.hpp"
#include "libtorrent/aux_/session_settings.hpp"
#include "libtorrent/io_service.hpp"
#include "libtorrent/alert.hpp"
#include "libtorrent/alert_types.hpp"
#include "libtorrent/disk_observer.hpp"
#include "libtorrent/platform_util.hpp" // for total_physical_ram

#include "libtorrent/aux_/disable_warnings_push.hpp"

#include <algorithm>
#include <boost/bind.hpp>
#include <boost/system/error_code.hpp>
#include <boost/shared_ptr.hpp>

#include "libtorrent/aux_/disable_warnings_pop.hpp"

namespace libtorrent
{
	namespace {

	// this is posted to the network thread
	void watermark_callback(std::vector<boost::weak_ptr<disk_observer> >* cbs)
	{
		if (cbs == NULL) return;
		boost::scoped_ptr<std::vector<boost::weak_ptr<disk_observer> > > holder(
			cbs);

		for (std::vector<boost::weak_ptr<disk_observer> >::iterator i = cbs->begin()
			, end(cbs->end()); i != end; ++i)
		{
			boost::shared_ptr<disk_observer> o = i->lock();
			if (o) o->on_disk();
		}
	}

	} // anonymous namespace

	disk_buffer_pool::disk_buffer_pool(int block_size, io_service& ios
		, boost::function<void()> const& trigger_trim)
		: m_block_size(block_size)
		, m_in_use(0)
		, m_max_use(64)
		, m_low_watermark((std::max)(m_max_use - 32, 0))
		, m_trigger_cache_trim(trigger_trim)
		, m_exceeded_max_size(false)
		, m_ios(ios)
		, m_cache_pool(NULL)
		, m_pool_size(0)
	{
#if TORRENT_USE_ASSERTS
		m_magic = 0x1337;
		m_settings_set = false;
#endif
	}

	disk_buffer_pool::~disk_buffer_pool()
	{
		TORRENT_ASSERT(m_magic == 0x1337);
#if TORRENT_USE_ASSERTS
		m_magic = 0;
#endif

		free_pool();
	}

	boost::uint32_t disk_buffer_pool::num_to_evict(int num_needed)
	{
		int ret = 0;

		mutex::scoped_lock l(m_pool_mutex);

		if (m_exceeded_max_size)
			ret = m_in_use - (std::min)(m_low_watermark, int(m_max_use - m_observers.size()*2));

		if (m_in_use + num_needed > m_max_use)
			ret = (std::max)(ret, int(m_in_use + num_needed - m_max_use));

		if (ret < 0) ret = 0;
		else if (ret > m_in_use) ret = m_in_use;

		return ret;
	}

	// checks to see if we're no longer exceeding the high watermark,
	// and if we're in fact below the low watermark. If so, we need to
	// post the notification messages to the peers that are waiting for
	// more buffers to received data into
	void disk_buffer_pool::check_buffer_level(mutex::scoped_lock& l)
	{
		TORRENT_ASSERT(l.locked());
		if (!m_exceeded_max_size || m_in_use > m_low_watermark) return;

		m_exceeded_max_size = false;

		std::vector<boost::weak_ptr<disk_observer> >* cbs
			= new std::vector<boost::weak_ptr<disk_observer> >();
		m_observers.swap(*cbs);
		l.unlock();
		m_ios.post(boost::bind(&watermark_callback, cbs));
	}

#if TORRENT_USE_ASSERTS
	bool disk_buffer_pool::is_disk_buffer(char* buffer
		, mutex::scoped_lock& l) const
	{
		TORRENT_ASSERT(m_magic == 0x1337);
		TORRENT_ASSERT(l.locked());
		TORRENT_UNUSED(l);

		if (m_cache_pool)
		{
			return buffer >= m_cache_pool && buffer < m_cache_pool
				+ boost::uint64_t(m_max_use) * m_block_size;
		}
		return false;
	}

	bool disk_buffer_pool::is_disk_buffer(char* buffer) const
	{
		mutex::scoped_lock l(m_pool_mutex);
		return is_disk_buffer(buffer, l);
	}
#endif // TORRENT_USE_ASSERTS

	char* disk_buffer_pool::allocate_buffer(char const* category)
	{
		mutex::scoped_lock l(m_pool_mutex);
		return allocate_buffer_impl(l, category);
	}

	// we allow allocating more blocks even after we exceed the max size,
	// but communicate back to the allocator (typically the peer_connection)
	// that we have exceeded the limit via the out-parameter "exceeded". The
	// caller is expected to honor this by not allocating any more buffers
	// until the disk_observer object (passed in as "o") is invoked, indicating
	// that there's more room in the pool now. This caps the amount of over-
	// allocation to one block per peer connection.
	char* disk_buffer_pool::allocate_buffer(bool& exceeded
		, boost::shared_ptr<disk_observer> o, char const* category)
	{
		mutex::scoped_lock l(m_pool_mutex);
		char* ret = allocate_buffer_impl(l, category);
		if (m_exceeded_max_size)
		{
			exceeded = true;
			if (o) m_observers.push_back(o);
		}
		return ret;
	}

// this function allocates buffers and
// fills in the iovec array with the buffers
	int disk_buffer_pool::allocate_iovec(file::iovec_t* iov, int iov_len)
	{
		mutex::scoped_lock l(m_pool_mutex);
		for (int i = 0; i < iov_len; ++i)
		{
			iov[i].iov_base = allocate_buffer_impl(l, "pending read");
			iov[i].iov_len = block_size();
			if (iov[i].iov_base == NULL)
			{
				// uh oh. We failed to allocate the buffer!
				// we need to roll back and free all the buffers
				// we've already allocated
				for (int j = 0; j < i; ++j)
					free_buffer_impl(static_cast<char*>(iov[j].iov_base), l);
				return -1;
			}
		}
		return 0;
	}

	void disk_buffer_pool::free_iovec(file::iovec_t* iov, int iov_len)
	{
		// TODO: perhaps we should sort the buffers here?
		mutex::scoped_lock l(m_pool_mutex);
		for (int i = 0; i < iov_len; ++i)
			free_buffer_impl(static_cast<char*>(iov[i].iov_base), l);
		check_buffer_level(l);
	}

	char* disk_buffer_pool::allocate_buffer_impl(mutex::scoped_lock& l
		, char const*)
	{
		TORRENT_ASSERT(m_settings_set);
		TORRENT_ASSERT(m_magic == 0x1337);
		TORRENT_ASSERT(l.locked());
		TORRENT_UNUSED(l);

		if (m_cache_pool == NULL)
		{
			error_code ec;
			allocate_pool(ec);
			if (ec) return NULL;
		}

		if (m_in_use >= m_high_watermark && !m_exceeded_max_size)
		{
			m_exceeded_max_size = true;
			m_trigger_cache_trim();
		}

		int const slot_index = m_free_blocks.find_first_set();
		if (slot_index < 0) return NULL;
		m_free_blocks.clear_bit(slot_index);

		char* ret = m_cache_pool + (slot_index * m_block_size);
		TORRENT_ASSERT(is_disk_buffer(ret, l));
#if TORRENT_USE_ASSERTS
		TORRENT_ASSERT(m_buffers_in_use.get_bit(slot_index) == false);
		m_buffers_in_use.set_bit(slot_index);
#endif
		++m_in_use;
		return ret;
	}

	void disk_buffer_pool::free_multiple_buffers(char** bufvec, int const numbufs)
	{
		char** const end = bufvec + numbufs;

		// TODO: 4 does this make sense?
		// sort the pointers in order to maximize cache hits
		std::sort(bufvec, end);

		mutex::scoped_lock l(m_pool_mutex);
		for (; bufvec != end; ++bufvec)
		{
			char* const buf = *bufvec;
			TORRENT_ASSERT(buf);
			free_buffer_impl(buf, l);
		}

		check_buffer_level(l);
	}

	void disk_buffer_pool::free_buffer(char* buf)
	{
		mutex::scoped_lock l(m_pool_mutex);
		free_buffer_impl(buf, l);
		check_buffer_level(l);
	}

	void disk_buffer_pool::set_settings(aux::session_settings const& sett
		, error_code& ec)
	{
		TORRENT_UNUSED(ec);

		mutex::scoped_lock l(m_pool_mutex);

		// we can only change the cache size when it's unused. Since the whole
		// cache is allocated as a single blob, to support resizing otherwise
		// would require creating copies of every block (at least dirty blocks)
		// and update all pointers in the block_cache.
		if (m_cache_pool && m_in_use > 0) return;

		int const cache_size = sett.get_int(settings_pack::cache_size);
		if (cache_size < 0)
		{
			boost::uint64_t phys_ram = total_physical_ram();
			boost::int64_t const gb = 1024 * 1024 * 1024;
			if (phys_ram <= gb) m_max_use = 256;
			else if (phys_ram <= 4 * gb) m_max_use = 1024;
			else m_max_use = 4096;
		}
		else
		{
			m_max_use = cache_size;
		}

		// 32 bit builds should capped below 2 GB of memory, even
		// when more actual ram is available, because we're still
		// constrained by the 32 bit virtual address space.
		if (sizeof(void*) == 4)
		{
			m_max_use = (std::min)(2 * 1024 * 1024 * 3 / 4 * 1024
				/ m_block_size, m_max_use);
		}

		// we need some space to store buffers in. If the cache size would be
		// literally zero, no operation could ever complete, as both reading and
		// writing requires buffers to transfer data between the disk I/O threads
		// and the network thread.
		if (m_max_use < 4) m_max_use = 4;

		int const max_queued_blocks = (std::max)(16
			, sett.get_int(settings_pack::max_queued_disk_bytes)
			/ m_block_size);

		m_low_watermark = m_max_use - max_queued_blocks;
		if (m_low_watermark < 0) m_low_watermark = 0;
		m_high_watermark = m_max_use - max_queued_blocks / 2;
		if (m_high_watermark < 0) m_high_watermark = 0;

		if (m_in_use >= m_max_use && !m_exceeded_max_size)
		{
			m_exceeded_max_size = true;
			m_trigger_cache_trim();
		}

#if TORRENT_USE_ASSERTS
		m_settings_set = true;
#endif

		if (m_cache_pool && m_pool_size != m_max_use * m_block_size)
		{
			TORRENT_ASSERT(m_in_use == 0);
			free_pool();
		}

		if (m_cache_pool == NULL)
		{
			allocate_pool(ec);
		}
	}

	void disk_buffer_pool::free_buffer_impl(char* buf, mutex::scoped_lock& l)
	{
		TORRENT_ASSERT(buf);
		TORRENT_ASSERT(m_magic == 0x1337);
		TORRENT_ASSERT(m_settings_set);
		TORRENT_ASSERT(is_disk_buffer(buf, l));
		TORRENT_ASSERT(l.locked());
		TORRENT_UNUSED(l);

		TORRENT_ASSERT(buf >= m_cache_pool);
		TORRENT_ASSERT(buf <  m_cache_pool + boost::uint64_t(m_max_use) * m_block_size);
		int const slot_index = (buf - m_cache_pool) / m_block_size;
		m_free_blocks.set_bit(slot_index);

#if TORRENT_USE_ASSERTS
		TORRENT_ASSERT(m_buffers_in_use.get_bit(slot_index));
		m_buffers_in_use.clear_bit(slot_index);
#endif

		--m_in_use;
	}

	void disk_buffer_pool::allocate_pool(error_code& ec)
	{
		if (m_cache_pool != NULL) return;

		m_pool_size = boost::uint64_t(m_max_use) * m_block_size;
		m_cache_pool = page_allocate(m_pool_size);

		// make sure it's page aligned
		TORRENT_ASSERT((size_t(m_cache_pool) & 0xfff) == 0);

		m_free_blocks.resize(m_max_use, true);

#if TORRENT_USE_ASSERTS
		m_buffers_in_use.resize(m_max_use);
		m_buffers_in_use.clear_all();
#endif
	}

	void disk_buffer_pool::free_pool()
	{
		page_free(m_cache_pool, m_pool_size);

		m_cache_pool = NULL;
		m_pool_size = 0;
		m_free_blocks.clear();
#if TORRENT_USE_ASSERTS
		m_buffers_in_use.clear();
#endif
	}

	// TODO: 4 figure out when this is called. Maybe this is a good time to
	// attempt running MADV_FREE
	void disk_buffer_pool::release_memory()
	{
		TORRENT_ASSERT(m_magic == 0x1337);
/*
#if defined MADV_FREE
		// tell the virtual memory system that we don't actually care
		// about the data in these pages anymore. If this block was
		// swapped out to the SSD, it (hopefully) means it won't have
		// to be read back in once we start writing our new data to it
		madvise(buf, m_block_size, MADV_FREE);
#elif defined MADV_DONTNEED && defined TORRENT_LINUX
		// rumor has it that MADV_DONTNEED is in fact destructive
		// on linux (i.e. it won't flush it to disk or re-read from disk)
		// http://kerneltrap.org/mailarchive/linux-kernel/2007/5/1/84410
		madvise(buf, m_block_size, MADV_DONTNEED);
#elif TORRENT_USE_VIRTUAL_ALLOC
		VirtualAlloc(addr, size, MEM_RESET);
#endif
*/
	}

}


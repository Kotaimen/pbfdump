/*
 * Stores a osm_id -> coordinate in a very space efficiently hash map.
 *
 * Current(2013) OSM planet dump will takes about 32GB memory.
 * Note Lonlat is truncated to 32bit accuracy. Set expected_size will
 * improve performance when dumping very large files.
 */

#include <stdio.h>
#include <string>

#include <sparsehash/sparse_hash_map>
#include <boost/python.hpp>
#include <Python.h>
using namespace boost::python;

struct DumpFileIOError {};

void translateException( const DumpFileIOError& x) {
	PyErr_SetString(PyExc_IOError, "Can't open dump file.");
};


/**
 * OsmId, signed int 64
 */
typedef long long OsmId;

const double SCALE_FACTOR = (1 << 31) / 180.;

/**
 * Coordinate Type, double coordinate is truncated to 180/2^32 and
 * stored as int32, so a struct only takes 32 bit instead of 64.
 * Keeping this as a POD so we can use deault serializer provided by
 * sparse hashing
 */
struct Coord {

	Coord() :
			lon(0), lat(0) {
	}
	Coord(double lon, double lat) :
			lon(int(lon * SCALE_FACTOR)), lat(int(lat * SCALE_FACTOR)) {
	}
	int lon;
	int lat;

	double get_lon() const {
		return lon / SCALE_FACTOR;
	}
	double get_lat() const {
		return lat / SCALE_FACTOR;
	}
};


/**
 * Node cache using google sparse hash map
 */
class SparseNodeCache {
	typedef google::sparse_hash_map<OsmId, Coord> MapType;
	MapType nodeMap;
public:

	SparseNodeCache(size_t expected_size=0) : nodeMap(expected_size){
	}

	virtual ~SparseNodeCache() {
	}

	std::string str() const {
		return "SparseNodeCache()";
	}

	object size() const {
		long_ size(nodeMap.size());
		return size;
	}

	void clear() {
		nodeMap.clear();
	}

	void insert(object osmid, tuple coord) {
		OsmId osmid_ = extract<OsmId>(osmid);

		Coord coord_ = Coord(extract<double>(coord[0]),
				extract<double>(coord[1]));
		nodeMap.insert(std::make_pair(osmid_, coord_));
	}

	object find(object osmid) const {
		OsmId osmid_ = extract<OsmId>(osmid);
		const MapType::const_iterator &it = nodeMap.find(osmid_);
		if (it == nodeMap.end()) {
			return object();
		} else {
			const Coord &coord = (*it).second;
			return make_tuple(coord.get_lon(), coord.get_lat());
		}
	}

	object findall(list osmid) const {
		list result;
		for (int i = 0; i < len(osmid); ++i) {
			result.append(this->find(osmid[i]));
		}
		return result;
	}

	void dump(const std::string &filename) {
		FILE *fp = fopen(filename.c_str(), "wb");
		if ( NULL == fp ) {
			throw DumpFileIOError();
		}
		nodeMap.serialize(MapType::NopointerSerializer(), fp);
		fclose(fp);
	}

	void load(const std::string &filename) {
		FILE *fp = fopen(filename.c_str(), "rb");
		if ( NULL == fp ) {
			throw DumpFileIOError();
		}
		nodeMap.unserialize(MapType::NopointerSerializer(), fp);
		fclose(fp);
	}
};

BOOST_PYTHON_MODULE(nodecache)
{
	register_exception_translator<DumpFileIOError>(translateException);

	class_<SparseNodeCache>("NodeCache")
		.def(init<size_t>())
		.def("__repr__", &SparseNodeCache::str)
		.def("size", &SparseNodeCache::size)
		.def("clear", &SparseNodeCache::clear)
		.def("insert", &SparseNodeCache::insert)
		.def("find",&SparseNodeCache::find)
		.def("findall",&SparseNodeCache::findall)
		.def("dump",&SparseNodeCache::dump)
		.def("load",&SparseNodeCache::load)
	;
}

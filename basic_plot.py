#!/usr/bin/env python
"""
install AWIPS from:

https://github.com/Unidata/python-awips

https://www.unidata.ucar.edu/blogs/developer/entry/awips-nexrad-level-3-rendered
"""
from argparse import ArgumentParser
from dateutil.parser import parse
from awips.dataaccess import DataAccessLayer
from awips import ThriftClient, RadarCommon
from dynamicserialize.dstypes.com.raytheon.uf.common.time import TimeRange
from dynamicserialize.dstypes.com.raytheon.uf.common.dataplugin.radar.request import GetRadarDataRecordRequest
import numpy as np

from pymap3d.vincenty import vreckon


nexrad = {}
nexrad["N0Q"] = {
    'id': 94,
    'unit':'dBZ',
    'name':'0.5 deg Base Reflectivity',
    'ctable': ['NWSStormClearReflectivity',-20., 0.5],
    'res': 1000.,
    'elev': '0.5'
}
nexrad["N0U"] = {
    'id': 99,
    'unit':'kts',
    'name':'0.5 deg Base Velocity',
    'ctable': ['NWS8bitVel',-100.,1.],
    'res': 250.,
    'elev': '0.5'
}


def download(site: str):

# %% SITE
    site = site.lower()

    DataAccessLayer.changeEDEXHost('edex-cloud.unidata.ucar.edu')
    request = DataAccessLayer.newDataRequest()
    request.setDatatype('radar')
    request.setLocationNames(site)

# %% TIME
    times = DataAccessLayer.getAvailableTimes(request)
    times = [parse(str(t)) for t in times]

    timerange = TimeRange(times[0], times[-1])

# %% REQUEST
    client = ThriftClient.ThriftClient('edex-cloud.unidata.ucar.edu')
    request = GetRadarDataRecordRequest()
    request.setTimeRange(timerange)
    request.setRadarId(site)

    code = 'N0Q'

    request.setProductCode(nexrad[code]['id'])
    request.setPrimaryElevationAngle(nexrad[code]['elev'])
    response = client.sendRequest(request)

    records = response.getData()
    print(f'found {len(records)} records at {site}')

    if not response.getData():
        raise OSError(f'data not available {timerange}')

    for rec in records:
        idra = rec.getHdf5Data()
        rdat, azdat, depVals, threshVals = RadarCommon.get_hdf5_data(idra)
        # dim = rdat.getDimension()
        lat,lon = float(rec.getLatitude()),float(rec.getLongitude())
        radials,rangeGates = rdat.getSizes()

        # Convert raw byte to pixel value
        array = np.array(rdat.getByteData())
        array[array < 0] = array[array < 0] + 256

        if azdat:
            azVals = azdat.getFloatData()
            az = np.array(RadarCommon.encode_radial(azVals))
            # dattyp = RadarCommon.get_data_type(azdat)
            az = np.append(az,az[-1])

        # header = RadarCommon.get_header(rec, format, rangeGates, radials, azdat, 'description')
        rng = np.linspace(0, rangeGates, rangeGates + 1) * nexrad[code]['res']

        lats = np.empty((rng.size, az.size))
        lons = np.empty_like(lats)
        for i,a in enumerate(az):
            lats, lons, _ = vreckon(lat, lon, rng, a)


def main():
    p = ArgumentParser()
    p.add_argument('site', help='NEXRAD site e.g. KMUX')
    p = p.parse_args()

    download(p.site)

if __name__ == '__main__':
    main()
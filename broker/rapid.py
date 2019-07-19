import pandas as pd
from broker.ztf_archive import _parse_data as psd
# from broker import rapid as ar # astrorapid
import astrorapid.astrorapid as ar
# from astrorapid.astrorapid.classify import Classify

def classify(light_curves, plot=True):
    """ Classifies alerts using RAPID.

    Args:
        light_curves (list of tuples): light_curve_info for multiple objects,
            where light_curve_info = (mjd, flux, fluxerr, passband, zeropoint, \
                                        photflag, ra, dec, objid, redshift, mwebv)

    Returns:
        List of predictions for each object.
    """

    # classification = ar.classify.Classify(light_curves)
    predictions = classification.get_predictions()
    print(predictions)

    if plot:
        # Plot classifications vs time
        classification.plot_light_curves_and_classifications()
        classification.plot_classification_animation()

    return predictions


def format_alert_data(alert):
    """ Takes a dict of observation data and returns a tuple formatted for RAPID classifier.

    Args:
        alert (dict):   Needs the following key:value pairs
                        'objectId'  : (int) unique for each object
                        'ra'        : (float) right ascension
                        'dec'       : (float) declination
                        'hostgal'   : (dict) ??? (used to calculate redshift)
                        'mwebv'     : (float) ???
                        'Obs'       : (list of dicts) one entry per epoch.
                                        Each dict needs the following key:value pairs
                                        'mjd'       : () modified julian date
                                        'flux'      : (float)
                                        'fluxerr'   : (float)
                                        'passband'  : (str)
                                        'zeropoint' : ()
                                        'photflag'  : ()

    Returns:
        Tuple of light curve data, Formatted as required for input to RAPID classifier.
    """

    redshift = 0 # fix this, calculate from alert['hostgal']

    # collect data from each epoch
    mjd, flux, fluxerr, passband, zeropoint, photflag = ([] for i in range(6))
    for c, cdat in enumerate(alert['Obs']):
        mjd.append(cdat['mjd'])
        flux.append(cdat['flux'])
        fluxerr.append(cdat['fluxerr'])
        passband.append(cdat['passband'])
        zeropoint.append(cdat['zeropoint'])
        photflag.append(cdat['photflag'])

    light_curve_info = (mjd, flux, fluxerr, passband, zeropoint, photflag, \
                        alert['ra'], alert['dec'], alert['objectId'], redshift, alert['mwebv'])
    return light_curve_info


def collect_ZTF_alerts(max_alerts=10):
    """ Iterates through previously downloaded ZTF alerts and returns
        list of light curve data for classification.

    Args:
        max_alerts (int): max number of alerts to collect. (Default: 10)

    Returns:
        light_curves (list of tuples): Formatted as required for input to RAPID classifier.

    """

    light_curves = [] # collect alert data for ar.Classify
    for a, alert in enumerate(psd.iter_alerts()):
        cand = alert['candidate']

        # Package alert data for format_alert_data()
        dict = {'objectId'  : alert['objectId'],
                'ra'        : cand['ra'],
                'dec'       : cand['dec'],
                'mwebv'     : 0 # fix this
                }

        # Host Galaxy: get the closest galaxy on the sky, used to calculate redshift
        zcols_pre = ['sgscore', 'sgmag', 'srmag', 'simag', 'szmag']
        for s in [1,2,3]:
            zcols = [ c + str(s) for c in zcols_pre ]
            if cand[zcols[0]] < 0.75: # fix this threshold value
                continue # if it's not a galaxy, move to the next source
            else:
                dict['hostgal'] = { zcols_pre[1]: cand[zcols[1]],
                                    zcols_pre[2]: cand[zcols[2]],
                                    zcols_pre[3]: cand[zcols[3]],
                                    zcols_pre[4]: cand[zcols[4]],
                                    } # fix this, may need to convert magnitudes
                break
            try:
                dict['hostgal']
            except:
                dict['hostgal'] = {} # fix this.. what to do when no known host gal

        # Observation epochs
        fid_dict = {1:'g', 2:'r', 3:'i'}
        dict['Obs'] = []
        for c, cdat in enumerate([cand] + alert['prv_candidates']):
            try:
                assert cdat['magpsf'] is not None # magpsf and sigmapsf are null for nondetections
                # check this, is it ok to skip nondetections? some objects have 0 previous detections
            except:
                print('Object {}, epoch {} has a nondetection.'.format(dict['objectId'], c))
                pass
            else:
                obs = { 'mjd'       : cdat['jd'] - 2400000.5, # check this
                        'flux'      : 10**( (22.5 - cdat['magpsf'])/ 2.5 ), # check this equ and use of magpsf
                        'fluxerr'   : 10**( (22.5 - cdat['sigmapsf'])/ 2.5 ), # check this
                        'passband'  : fid_dict[cdat['fid']], # fix this, no entry for z band
                        'zeropoint' : cdat['magzpsci'], # check this
                        'photflag'  : 0 # fix this
                }
                dict['Obs'].append(obs)


        # Format data for RAPID classifier
        light_curves.append(format_alert_data(dict))

        if (a > max_alerts) & (a > 0):
            break

    return light_curves




# alert['candidate'].keys() = ['jd', 'fid', 'pid', 'diffmaglim', 'pdiffimfilename', 'programpi',
# 'programid', 'candid', 'isdiffpos', 'tblid', 'nid', 'rcid', 'field', 'xpos', 'ypos',
# 'ra', 'dec', 'magpsf', 'sigmapsf','chipsf', 'magap', 'sigmagap',
# 'distnr', 'magnr', 'sigmagnr', 'chinr', 'sharpnr', 'sky', 'magdiff', 'fwhm', 'classtar',
# 'mindtoedge', 'magfromlim', 'seeratio', 'aimage', 'bimage', 'aimagerat', 'bimagerat',
# 'elong', 'nneg', 'nbad', 'rb', 'ssdistnr', 'ssmagnr', 'ssnamenr', 'sumrat', 'magapbig',
# 'sigmagapbig','ranr', 'decnr', 'sgmag1', 'srmag1', 'simag1', 'szmag1', 'sgscore1',
# 'distpsnr1', 'ndethist', 'ncovhist', 'jdstarthist', 'jdendhist', 'scorr', 'tooflag',
# 'objectidps1', 'objectidps2', 'sgmag2', 'srmag2', 'simag2', 'szmag2', 'sgscore2', 'distpsnr2',
# 'objectidps3', 'sgmag3', 'srmag3', 'simag3', 'szmag3', 'sgscore3', 'distpsnr3',
# 'nmtchps', 'rfid', 'jdstartref', 'jdendref', 'nframesref', 'rbversion', 'dsnrms', 'ssnrms',
# 'dsdiff', 'magzpsci', 'magzpsciunc', 'magzpscirms', 'nmatches', 'clrcoeff', 'clrcounc',
# 'zpclrcov', 'zpmed', 'clrmed', 'clrrms', 'neargaia', 'neargaiabright', 'maggaia', 'maggaiabright',
# 'exptime', 'drb', 'drbversion']
#
# alert['prv_candidates'][0].keys() = ['jd', 'fid', 'pid', 'diffmaglim', 'pdiffimfilename',
# 'programpi', 'programid', 'candid', 'isdiffpos', 'tblid', 'nid', 'rcid', 'field',
# 'xpos', 'ypos', 'ra', 'dec', 'magpsf', 'sigmapsf','chipsf', 'magap', 'sigmagap',
# 'distnr', 'magnr', 'sigmagnr', 'chinr', 'sharpnr', 'sky', 'magdiff', 'fwhm', 'classtar',
# 'mindtoedge', 'magfromlim', 'seeratio', 'aimage', 'bimage', 'aimagerat', 'bimagerat','elong',
# 'nneg', 'nbad', 'rb', 'ssdistnr', 'ssmagnr', 'ssnamenr', 'sumrat', 'magapbig', 'sigmagapbig',
# 'ranr', 'decnr', 'scorr', 'magzpsci', 'magzpsciunc', 'magzpscirms', 'clrcoeff', 'clrcounc',
# 'rbversion'])

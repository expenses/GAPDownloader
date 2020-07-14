#!/usr/bin/env python3
# coding: utf-8
import asyncio
import tile_fetch
import sys
import pandas as pd
from random import randint
from time import sleep
 
def main():
    import argparse
 
    parser = argparse.ArgumentParser(description='Google Arts & Culture website downloader')
    parser.add_argument('-a','--add_url', type=str, nargs='?', help='Add new Arts & Culture URLs.',
                        action='store', dest='url')
    parser.add_argument('-z','--zoom', type=int, nargs=1,
                        help='Zoom level to fetch, can be negative. Will print zoom levels if omitted')
    parser.add_argument('-q','--quality', type=int, nargs='?', default=90,
                        help='Compression level from 0-95. Higher is better.')
    parser.add_argument('-d', '--download', help="Downloads all remaining links in queue.",action="store_true", default=False)
    parser.add_argument('-b', '--batch-add', type=str, nargs=1, help="Adds a list of URL's to the queue from a csv file of URLs.", action="store", dest='csv')
    args = parser.parse_args()
 
    df = None
    try:
        df = pd.read_csv("dlcache", index_col=0)
    except:
        print("No cache found. Setting up a new one.")
        df = pd.DataFrame(columns=['url', 'quality', 'downloaded'])
 
    if args.csv:
        url_df = pd.read_csv(args.csv[0])
        for u in url_df['url']:
            print("######### Processing '{}'".format(u))
          
            img_id = u[-(len(u)-u.rfind("/")-1):]
            print(img_id)
            
            if not (img_id in df.index):
                assert 0 <= args.quality <= 95, "Image quality must be between 0 and 95"
                df.loc[img_id] = {'url':u, 'quality':args.quality, "downloaded":False}
                print("######### Added to queue.")
            else:
                print("Image already in list. Ignoring the URL.")
   
    if args.url:
        print("######### Processing '{}'".format(args.url))
        u = args.url
        img_id = u[-(len(u)-u.rfind("/")-1):]
        if not (img_id in df.index):
            df.loc[image_info.image_id] = {'url':args.url, 'quality':args.quality, "downloaded":False}
            print("######### Added to queue.")
        else:
            print("Image already in list. Ignoring the URL.")
 
    if args.download:      
        print("######### Starting download")
        for row in df.loc[df['downloaded'] == False].iterrows(): 
            print(row[1]['url'])          
            image_info = None        
            
            try:
                image_info = tile_fetch.ImageInfo(row[1]['url'])
            except:
                print("Invalid url.")
                valid_url = False
            
            assert 0 <= args.quality <= 95, "Image quality must be between 0 and 95"
            
            if image_info:
                 if args.zoom:
                     zoom = args.zoom
                     try:
                         assert 0 <= zoom < len(image_info.tile_info)
                     except:
                         print('No valid zoom level.')
                 else:
                     zoom = len(image_info.tile_info)-1
                     print("Defaulting to highest zoom level ({}).".format(zoom))
                 
                 ## Ensuring image resolution fits in JPEG - two pass 
                 if image_info.tile_info[zoom].size[0] > 65535 or image_info.tile_info[zoom].size[1] > 65535:
                     print(
                        'Zoom level {r} too high for JPEG output, using next zoom level {next_zoom} instead'.format(
                            r=zoom,
                            next_zoom=zoom-1)
                     )
                     zoom = zoom-1

                 if image_info.tile_info[zoom].size[0] > 65535 or image_info.tile_info[zoom].size[1] > 65535:
                     print(
                        'Zoom level {r} *still* too high for JPEG output, using next zoom level {next_zoom} instead'.format(
                            r=zoom,
                            next_zoom=zoom-1)
                     )
                     zoom = zoom-1
                 
                 print("Using zoom level {}.".format(zoom))
        
            
            coro = tile_fetch.load_tiles(image_info, zoom, image_info.image_name, row[1]['quality'])
            loop = asyncio.get_event_loop()
            loop.run_until_complete(coro)
            df.at[image_info.image_id, "downloaded"] = True
            print("Download successful. Sleeping before next download...")
            sleep(randint(30,40))
        print("######### Finished download")
 
    df.to_csv('dlcache')
 
if __name__ == '__main__':
    main()
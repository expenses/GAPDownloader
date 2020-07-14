#!/usr/bin/env python3
# coding: utf-8
import asyncio
import base64
import hmac
import io
import itertools
import re
import shutil
import string
import unidecode
import urllib.parse
import urllib.request
from pathlib import Path

import aiohttp
from PIL import Image
from lxml import etree
from pyexiv2 import Image as TaggedImage

import async_tile_fetcher
from decryption import decrypt

IV = bytes.fromhex("7b2b4e23de2cc5c5")


def compute_url(path, token, x, y, z):
    """
    >>> path = b'wGcDNN8L-2COcm9toX5BTp6HPxpMPPPuxrMU-ZL-W-nDHW8I_L4R5vlBJ6ITtlmONQ'
    >>> token = b'KwCgJ1QIfgprHn0a93x7Q-HhJ04'
    >>> compute_url(path, token, 0, 0, 7)
    'https://lh3.googleusercontent.com/wGcDNN8L-2COcm9toX5BTp6HPxpMPPPuxrMU-ZL-W-nDHW8I_L4R5vlBJ6ITtlmONQ=x0-y0-z7-tHeJ3xylnSyyHPGwMZimI4EV3JP8'
    """
    sign_path = b'%s=x%d-y%d-z%d-t%s' % (path, x, y, z, token)
    encoded = hmac.new(IV, sign_path, 'sha1').digest()
    signature = base64.b64encode(encoded, b'__')[:-1]
    url_bytes = b'https://lh3.googleusercontent.com/%s=x%d-y%d-z%d-t%s' % (path, x, y, z, signature)
    return url_bytes.decode('utf-8')


class ImageInfo(object):
    RE_URL_PATH_TOKEN = re.compile(rb']\r?\n,"(//[^"/]+/[^"/]+)",(?:"([^"]+)"|null)', re.MULTILINE)

    def __init__(self, url):
        page_source = urllib.request.urlopen(url).read()

        self.metadata = {'Xmp.xmp.URL': url}
        for item in html.fromstring(page_source).cssselect('[id^="metadata"] li'):
            text = item.text_content()
            # XMP metadata needs to be under the Xmp.xml section
            # removes and non-word character from the title as they invalid for metadata tag names
            key = 'Xmp.xmp.' + re.sub(r'\W', '', text[:text.find(':')])
            self.metadata[key] = text[text.find(':') + 1:].strip()

        match = self.RE_URL_PATH_TOKEN.search(page_source)
        if match is None:
            raise ValueError("Unable to find google arts image token")
        url_no_proto, token = match.groups()
        assert url_no_proto, "Unable to extract required information from the page"
        self.path = url_no_proto.rsplit(b'/', 1)[1]
        self.token = token or b''
        url_path = urllib.parse.unquote_plus(urllib.parse.urlparse(url).path)
        self.image_slug, image_id = url_path.split('/')[-2:]
        self.image_name = unidecode.unidecode(string.capwords(self.image_slug.replace("-"," ")))
        self.image_id = image_id

        meta_info_url = "https:{}=g".format(url_no_proto.decode('utf8'))
        meta_info_tree = etree.fromstring(urllib.request.urlopen(meta_info_url).read())
        self.tile_width = int(meta_info_tree.attrib['tile_width'])
        self.tile_height = int(meta_info_tree.attrib['tile_height'])
        self.tile_info = [
            ZoomLevelInfo(self, i, attrs.attrib)
            for i, attrs in enumerate(meta_info_tree.xpath('//pyramid_level'))
        ]

    def url(self, x, y, z):
        return compute_url(self.path, self.token, x, y, z)

    def __repr__(self):
        return '{} - zoom levels:\n{}'.format(
            self.image_slug,
            '\n'.join(map(str, self.tile_info))
        )


class ZoomLevelInfo(object):
    def __init__(self, img_info, level_num, attrs):
        self.num = level_num
        self.num_tiles_x = int(attrs['num_tiles_x'])
        self.num_tiles_y = int(attrs['num_tiles_y'])
        self.empty_x = int(attrs['empty_pels_x'])
        self.empty_y = int(attrs['empty_pels_y'])
        self.img_info = img_info

    @property
    def size(self):
        return (
            self.num_tiles_x * self.img_info.tile_width - self.empty_x,
            self.num_tiles_y * self.img_info.tile_height - self.empty_y
        )

    @property
    def total_tiles(self):
        return self.num_tiles_x * self.num_tiles_y

    def __repr__(self):
        return 'level {level.num:2d}: {level.size[0]:6d} x {level.size[1]:6d} ({level.total_tiles:6d} tiles)'.format(
            level=self)


async def fetch_tile(session, image_info, tiles_dir, x, y, z):
    file_path = tiles_dir / ('%sx%sx%s.jpg' % (x, y, z))
    image_url = image_info.url(x, y, z)
    encrypted_bytes = await async_tile_fetcher.fetch(session, image_url, file_path)
    return x, y, encrypted_bytes


async def load_tiles(info, z=-1, outfile=None, quality=90):
    if z >= len(info.tile_info):
        print(
            'Invalid zoom level {z}. '
            'The maximum zoom level is {max}, using that instead.'.format(
                z=z,
                max=len(info.tile_info) - 1)
        )
        z = len(info.tile_info) - 1

    ## Ensuring image resolution fits in JPEG - two pass 
    if info.tile_info[z].size[0] > 65535 or info.tile_info[z].size[1] > 65535:
        print(
            'Zoom level {r} too high for JPEG output, using next zoom level {next_z} instead'.format(
                r=z,
                next_z=z-1)
        )
        z = z-1
        
    if info.tile_info[z].size[0] > 65535 or info.tile_info[z].size[1] > 65535:
        print(
            'Zoom level {r} *still* too high for JPEG output, using next zoom level {next_z} instead'.format(
                r=z,
                next_z=z-1)
        )
        z = z-1

    z %= len(info.tile_info)  # keep 0 <= z < len(tile_info)
    level = info.tile_info[z]

    img = Image.new(mode="RGB", size=level.size)

    tiles_dir = Path(info.image_name)
    tiles_dir.mkdir(exist_ok=True)

    async with aiohttp.ClientSession() as session:
        awaitable_tiles = [
            fetch_tile(session, info, tiles_dir, x, y, z)
            for (x, y) in itertools.product(
                range(level.num_tiles_x),
                range(level.num_tiles_y))
        ]
        print("Downloading tiles...")
        tiles = await async_tile_fetcher.gather_progress(awaitable_tiles)

    for x, y, encrypted_bytes in tiles:
        clear_bytes = decrypt(encrypted_bytes)
        tile_img = Image.open(io.BytesIO(clear_bytes))
        img.paste(tile_img, (x * info.tile_width, y * info.tile_height))

  ## Try to extract author name ("Creator"/"Painter") and date ("Date Created"/"Date") from metadata
    author = "0"
    date = ""
    for key, value in info.metadata.items():
        if key.lower() == "xmp.xmp.creator" or key.lower() == "xmp.xmp.painter":
            # Avoiding non-ASCII characters in the painter/creator name
            author = unidecode.unidecode(value)
        elif key.lower() == "xmp.xmp.date" or key.lower() == "xmp.xmp.datecreated":
            # Avoiding "/" in the date (year), especially when multiple dates are given
            date = value.replace('/','-')
            
    # Taking out the author's name from the image name - authors name is appended later
    modified_image_name = info.image_name[0:len(info.image_name)-len(author)-1]
    
    final_image_filename = (author + ' - ' + date + ' - ' + modified_image_name + ' - ' +info.image_id + '.jpg')
    img.save(final_image_filename, quality=quality, subsampling=0, optimize=True)
    xmp_file_obj = TaggedImage(final_image_filename)

    # writes key:value one at a time, which is heavier on writes,
    # but far more robust.
    for key, value in info.metadata.items():
        try:
            xmp_file_obj.modify_xmp({key: value})
        except RuntimeError:
            print(f'Failed to add add XMP tag with key "{key}" with value "{value}"')
    shutil.rmtree(tiles_dir)
    print("Saved the result as " + final_image_filename)


def main():
    import argparse

    parser = argparse.ArgumentParser(description='Download all image tiles from Google Arts and Culture website')
    parser.add_argument('url', type=str, nargs='?', help='an artsandculture.google.com url')
    parser.add_argument('--zoom', type=int, nargs='?',
                        help='Zoom level to fetch, can be negative. Will print zoom levels if omitted')
    parser.add_argument('--outfile', type=str, nargs='?',
                        help='The name of the file to create.')
    parser.add_argument('--quality', type=int, nargs='?', default=90,
                        help='Compression level from 0-95. Higher is better.')
    args = parser.parse_args()

    assert 0 <= args.quality <= 95, "Image quality must be between 0 and 95"
    url = args.url or input("Enter the url of the image: ")

    print("Downloading image meta-information...")
    image_info = ImageInfo(url)

    zoom = args.zoom
    if zoom is None:
        print(image_info)
        while True:
            try:
                zoom = int(input("Which level do you want to download? "))
                assert 0 <= zoom < len(image_info.tile_info)
                break
            except (ValueError, AssertionError):
                print("Not a valid zoom level.")

    coro = load_tiles(image_info, zoom, args.outfile, args.quality)
    loop = asyncio.get_event_loop()
    loop.run_until_complete(coro)


if __name__ == '__main__':
    main()

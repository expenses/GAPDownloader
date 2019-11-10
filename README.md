# GAP decoder

This project aims at making it possible to download images from
[google art and culture](https://artsandculture.google.com/)
(formerly Google Art Project).

## How to use

> **Note** : If you are not comfortable with installing a scripting language on your computer, you can use instead of gapdecoder:
> - [dezoomify](https://ophir.alwaysdata.net/dezoomify/dezoomify.html), which can be used online without downloading anything to your computer, but limits the maximum size of downloaded images. 
> - [dezoomify-rs](https://github.com/lovasoa/dezoomify-rs#dezoomify-rs), which comes as ready-to-use executable. 


First, install [python 3](https://www.python.org/) on your system,
and install the dependencies:

```bash
pip3 install -r requirements.txt 
```

Then, run the code

```bash
python3 tile_fetch.py --zoom 4 "https://artsandculture.google.com/asset/the-water-carrier-la-aguadora/UwE2fGsMlWHuMg"
```

You can of course change the zoom level and the URL.
If you omit the zoom level, the script will display the list of available levels.

Run with the '-h' flag for a list of available commands.

## Technical details

This project required reverse-engineering google's code to find 
the protection measures in place and circumvent them.
Here is what was found.

### Tile URLs

The tile URLs are signed using HMAC.
[See the details](./tile_fetch.py)

### Tile images

The tile images are encoded using AES 128 CBC.
[See the details](./decryption.py)

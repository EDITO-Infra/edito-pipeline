Overview
========

This project involves the processing of each Layer for a given Layer 
Collection from Metagis into a STAC Catalog that is compatible with the 
EDITO STAC Catalog. It has been designed foremost for the EDITO Layer 
Collection, but can process other Layer Collections such as Bio-Oracle. 
The pipeline reforms the metadata that comes from Metagis, or attempts 
to retrieve it from elsewhere, to make items sufficient for the EDITO 
STAC Catalog. Where necessary (configured in Metagis), data products 
from given layers in the Layer Collection can also be converted into an 
ARCO format and these features will also be added to the STAC Catalog.

First, create a LayerCatalog, composed of the Layers from a Layer 
Collection. Then for each Layer obtain at least one native data product, 
and the necessary metadata to make a 'native STAC' feature. The subtheme 
(variable family), subsubtheme (collection), and dataset title (item) 
govern how this STAC feature fits in the STAC catalog. For Layers where 
'attributes' have been configured in Metagis, at least one native data 
product can be converted into an ARCO format (.zarr or .parquet) and 
ARCO assets can be added to 'ARCO STAC' features. These features use the 
same subtheme (variable family), but then the ARCO attributes determine 
which collection the item(s) fall into, and the spatial and temporal 
bounds to make the STAC item add the feature to the STAC catalog.
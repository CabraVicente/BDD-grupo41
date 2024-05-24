import db_reset

### El orden es de suma importancia!!! ###

import loaders.cliente
print("Loaded clientes")
import loaders.comuna_direccion # Necesita que exista la tabla Cliente
print("Loaded comunas y direccion")
import loaders.deliverymanager
print("Loaded delivery managers")
import loaders.despachador
print("Loaded despachadores")
#import loaders.pedido # Le falta todavia xd
import loaders.restaurante_sucursal
print("Loaded restaurantes y sucursales")
import loaders.plato # Necesita que exista la tabla Restaurante
print("Loaded platos")
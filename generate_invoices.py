import sys
import os
import os.path
import argparse
import sqlite3
import shutil
import random
import math
import datetime as dt
from dataclasses import dataclass
from typing import Dict, List


@dataclass
class CumFreqRow:
    value      : object
    cumfreq : int

@dataclass
class CumFreqTable:
    rows        : List[CumFreqRow]
    maxcumfreq  : int
    
    @classmethod
    def new(klass):
        return klass(rows=[], maxcumfreq=0)
        
    def add_row(self, value, freq):
        self.maxcumfreq += freq
        row = CumFreqRow(value, self.maxcumfreq)
        self.rows.append(row)
    
    def pick(self):
        pick_cumfreq = random.randint(0, self.maxcumfreq)
        pick = self.rows[0]
        for row in self.rows:
            if row.cumfreq > pick_cumfreq:
                break
            pick = row
        return pick.value
        
class Db:
    
    SQL_READ_MUSIC_DATA = """
        SELECT	a.GenreId 	as genre_id
        ,		a.Name 		as genre
        ,		d.ArtistId 	as artist_id
        ,		d.Name 		as artist
        ,		c.AlbumId 	as album_id
        ,		c.Title 	as album
        ,		b.TrackId 	as track_id
        ,		b.Name 		as track
        ,       b.UnitPrice as unit_price
        FROM	genres a
                --
                INNER JOIN tracks b
                ON	a.GenreId   = b.GenreId 
                --
                INNER JOIN albums c
                ON	b.AlbumId   = c.AlbumId 
                --
                INNER JOIN artists d
                ON	c.ArtistId  = d.ArtistId 
                --
        ORDER	BY 1, 3, 5, 7
    """
    SQL_LAST_ROWID = "SELECT last_insert_rowid();"
    SQL_INSERT_CUSTOMER = """
        INSERT INTO customers(
            FirstName
        ,   LastName
        ,   Company
        ,   Address
        ,   City
        ,   State
        ,   Country
        ,   PostalCode
        ,   Phone
        ,   Fax
        ,   Email
        ,   SupportRepId
        ) VALUES (
            ? -- FirstName
        ,   ? -- LastName
        ,   ? -- Company
        ,   ? -- Address
        ,   ? -- City
        ,   ? -- State
        ,   ? -- Country
        ,   ? -- PostalCode
        ,   ? -- Phone
        ,   ? -- Fax
        ,   ? -- Email
        ,   ? -- SupportRepId        
        );
    """
    
    SQL_INSERT_INVOICE = """
        INSERT INTO invoices (
            CustomerId
        ,   InvoiceDate
        ,   BillingAddress
        ,   BillingCity
        ,   BillingState
        ,   BillingCountry
        ,   BillingPostalCode
        ,   Total
        ) VALUES (
            ? -- CustomerId
        ,   ? -- InvoiceDate
        ,   ? -- BillingAddress
        ,   ? -- BillingCity
        ,   ? -- BillingState
        ,   ? -- BillingCountry
        ,   ? -- BillingPostalCode
        ,   ? -- Total
        );
    """
    
    SQL_UPDATE_INVOICE_TOTAL = """
        UPDATE  invoices SET Total = Total + ? WHERE InvoiceId = ?;
    """
    
    SQL_INSERT_INVOICE_LINE = """
        INSERT INTO invoice_items(
            InvoiceId
        ,   TrackId
        ,   UnitPrice
        ,   Quantity
        ) VALUES (
            ? -- InvoiceId
        ,   ? -- TrackId
        ,   ? -- UnitPrice
        ,   ? -- Quantity
        );
    """
    def __init__(self, dbfile):
        self.dbfile = dbfile
        self.conn = None
    
    def open(self):
        self.conn = sqlite3.connect(self.dbfile)
    
    def close(self):
        self.conn.close()
        self.conn = None
    
    def commit(self):
        self.conn.commit()
    
    def rollback(self):
        self.conn.rollback()
    
    def fetch_music_data(self):
        cursor = self.conn.cursor()
        cursor.execute(self.SQL_READ_MUSIC_DATA)
        rows = cursor.fetchall()
        return MusicData.from_rows(rows)
    
    def insert_customer(self, c):
        params = (
            c.first_name
        ,   c.last_name
        ,   c.company
        ,   c.address
        ,   c.city
        ,   c.state
        ,   c.country
        ,   c.postal_code
        ,   c.phone
        ,   c.fax
        ,   c.email
        ,   c.support_rep_id
        )
        cursor = self.conn.cursor()
        cursor.execute(self.SQL_INSERT_CUSTOMER, params)
        cursor.execute(self.SQL_LAST_ROWID)
        row = cursor.fetchone()
        c.id = row[0]
        del cursor

    def insert_invoice(self, i):
        params = (
            i.customer_id
        ,   i.invoice_date
        ,   i.address
        ,   i.city
        ,   i.state
        ,   i.country
        ,   i.postal_code
        ,   i.total
        )
        cursor = self.conn.cursor()
        cursor.execute(self.SQL_INSERT_INVOICE, params)
        cursor.execute(self.SQL_LAST_ROWID)
        row = cursor.fetchone()
        i.id = row[0]
        del cursor
    
    def insert_invoice_line(self, il):
        params = (
            il.invoice_id
        ,   il.track_id
        ,   il.unit_price
        ,   il.quantity
        )
        cursor = self.conn.cursor()
        cursor.execute(self.SQL_INSERT_INVOICE_LINE, params)
        cursor.execute(self.SQL_LAST_ROWID)
        row = cursor.fetchone()
        il.id = row[0]
        params = (
            il.unit_price * il.quantity
        ,   il.id
        )
        cursor.execute(self.SQL_UPDATE_INVOICE_TOTAL, params)
        del cursor

@dataclass 
class MusicData:
    genre_id    : int
    genre       : str
    artist_id   : int
    artist      : str
    album_id    : int
    album       : str
    track_id    : int
    track       : str
    unit_price  : float
    
    @classmethod
    def from_rows(klass, rows):
        result = []
        for row in rows:
            obj = klass(*row)
            result.append(obj)
        return result

@dataclass
class State:
    genres         : Dict[ int, "Genre"    ]
    artists        : Dict[ int, "Artist"   ]
    albums         : Dict[ int, "Album"    ]
    tracks         : Dict[ int, "Track"    ]
    customers      : Dict[ int, "Customer" ]
    customer_ids   : List[ int             ]
    genre_cumfreqs : CumFreqTable
    
    NUM_INVOICE_LINES_MU    = math.log(2)
    NUM_INVOICE_LINES_SIGMA = 0.75
    
    def __repr__(self):
        return "<State>"

    def __str__(self):
        return "<State>"
        
    @classmethod
    def new(klass):
        return klass(
            genres          = {}
        ,   artists         = {}
        ,   albums          = {}
        ,   tracks          = {}
        ,   customers       = {}
        ,   customer_ids    = []
        ,   genre_cumfreqs  = CumFreqTable.new()
        )
        
    def get_genre(self, genre_id):
        return self.genres.get(genre_id, None)
    
    def get_artist(self, artist_id):
        return self.artists.get(artist_id, None)
    
    def get_album(self, album_id):
        return self.albums.get(album_id, None)

    def get_track(self, track_id):
        return self.tracks.get(track_id, None)
        
    def add_genre(self, genre):
        assert genre.id not in self.genres
        self.genres[genre.id] = genre
    
    def get_customer(self, customer_id):
        return self.customers.get(customer_id, None)
        
    def add_artist(self, artist):
        assert artist.id not in self.artists
        self.artists[artist.id] = artist

    def add_album(self, album):
        assert album.id not in self.albums
        artist = self.get_artist(album.artist_id)
        assert artist is not None
        self.albums[album.id] = album
        artist.album_ids.append(album.id)

    def add_track(self, track):
        assert track.id not in self.tracks
        album = self.get_album(track.album_id)
        assert album is not None
        genre = self.get_genre(track.genre_id)
        assert genre is not None
        self.tracks[track.id] = track
        album.track_ids.append(track.id)
        genre.track_ids.append(track.id)
        
    
    def add_customer(self, customer):
        self.customers[customer.id] = customer
        self.customer_ids.append(customer.id)
        
    def ensure_genre(self, entry):
        obj = self.get_genre(entry.genre_id)
        if obj is None:
            obj = Genre.from_entry(entry, self)
            self.add_genre(obj)
        return obj

    def ensure_artist(self, entry):
        obj = self.get_artist(entry.artist_id)
        if obj is None:
            obj = Artist.from_entry(entry, self)
            self.add_artist(obj)
        return obj
        
    def ensure_album(self, entry):
        obj = self.get_album(entry.album_id)
        if obj is None:
            obj = Album.from_entry(entry, self)
            self.add_album(obj)
        return obj
    
    def ensure_track(self, entry):
        obj = self.get_track(entry.track_id)
        if obj is None:
            obj = Track.from_entry(entry, self)
            self.add_track(obj)
        return obj
    
    def process_entry(self, entry):
        self.ensure_genre(entry)
        self.ensure_artist(entry)
        self.ensure_album(entry)
        self.ensure_track(entry)
    
    def fill_genre_cumfreqs(self):
        for genre in self.genres.values():
            self.genre_cumfreqs.add_row(genre.id, genre.track_count)
            
    def show(self):
        for artist in self.artists.values():
            print(artist.id, '-', artist.name)
            for album_id in artist.album_ids:
                album = self.get_album(album_id)
                print('\t', album.id, '-', album.name)
                for track_id in album.track_ids:
                    track = self.get_track(track_id)
                    genre = self.get_genre(track.genre_id)
                    print('\t\t', track.id, '-', genre.name, '-', track.name)
    
    def pick_genre_preference(self, n):
        prefs = []
        for i in range(n):
            genre_id = self.genre_cumfreqs.pick()
            if genre_id not in prefs:
                prefs.append(genre_id)
        return prefs
    
    def create_customer(self, db):
        c = Customer.random(self)
        db.insert_customer(c)
        self.add_customer(c)
        return c
    
    def sample_customer(self):
        customer_id = random.choice(self.customer_ids)
        return self.customers[customer_id]

    def sample_track_for(self, customer):
        genre_id    = random.choice(customer.preferences)
        genre       = self.get_genre(genre_id)
        track_id    = random.choice(genre.track_ids)
        if track_id in customer.tracks_bought:
            return None
        return self.get_track(track_id)
                
    def create_invoice(self, db, date):
        customer = self.sample_customer()
        tracks   = []
        
        r = random.lognormvariate(self.NUM_INVOICE_LINES_MU, self.NUM_INVOICE_LINES_SIGMA)
        num_lines = 1 + int(r)
        
        for i in range(num_lines):
            track = self.sample_track_for(customer)
            if track is None:
                continue
            tracks.append(track)
        
        if not tracks:
            return 0
            
        invoice = Invoice.new(date, customer)
        db.insert_invoice(invoice)
        for track in tracks:
            invoice_line = InvoiceLine.new(invoice, track)
            db.insert_invoice_line(invoice_line)
            customer = self.get_customer(invoice.customer_id)
            customer.tracks_bought.append(track.id)
        return 1
        
@dataclass
class Genre:
    id: int
    name: str
    track_ids: List[int]
    state: State
    
    @classmethod
    def from_entry(klass, entry, state):
        return klass(
            id          = entry.genre_id
        ,   name        = entry.genre
        ,   track_ids   = []
        ,   state       = state
        )
    
    @property
    def track_count(self):
        return len(self.track_ids)
    
@dataclass
class Artist:
    id: int
    name: str
    album_ids: List[int]
    state: State

    @classmethod
    def from_entry(klass, entry, state):
        return klass(
            id          = entry.artist_id
         ,  name        = entry.artist
         ,  album_ids   = []
         ,  state       = state
        )
        
@dataclass
class Album:
    id: int
    name: str
    artist_id: int
    track_ids: List[int]
    state: State

    @classmethod
    def from_entry(klass, entry, state):
        return klass(
            id          = entry.album_id
        ,   name        = entry.album
        ,   artist_id   = entry.artist_id
        ,   track_ids   = []
        ,   state       = state
        )
        
@dataclass
class Track:
    id: int
    name: str
    album_id: int
    genre_id: int
    unit_price: float
    state: State

    @classmethod
    def from_entry(klass, entry, state):
        return klass(
            id          = entry.track_id
        ,   name        = entry.track
        ,   album_id    = entry.album_id
        ,   genre_id    = entry.genre_id
        ,   unit_price  = entry.unit_price
        ,   state       = state
        )

@dataclass
class Customer:
    # db fields
    id	           : int
    first_name	   : str
    last_name	   : str
    company	       : str
    address	       : str
    city	       : str
    state	       : str
    country	       : str
    postal_code	   : str
    phone	       : str
    fax	           : str
    email	       : str
    support_rep_id : int
    # not persisted
    db_state       : State
    preferences    : List[int]
    tracks_bought  : List[int]
    
    DEFAULT_COMPANY         = None
    DEFAULT_ADDRESS         = 'Rua das Palmeiras, n. 7'
    DEFAULT_POSTAL_CODE     = '87654-321'
    DEFAULT_PHONE           = '+55 (21) 99234-5678'
    DEFAULT_EMAIL           = 'foo.bar@gmail.com'
    DEFAULT_SUPPORT_REP_ID  = 3
    PREFERENCE_COUNT        = 5
    FIRST_NAMES = [
        'gabriela', 'ana', 'amanda', 'fernanda', 'júlia', 'beatriz', 'mariana', 'larissa', 'camila', 'leticia', 
        'juliana', 'natália', 'thais', 'vitoria', 'laura', 'jéssica', 'rafaela', 'luana', 'bruna', 'barbara', 
        'maria', 'isabela', 'anna', 'carolina', 'brenda', 'lívia', 'aline', 'milena', 'giovanna', 'ana clara', 
        'victória', 'maria clara', 'daniela', 'gabrielle', 'raquel', 'marcela', 'andressa', 'luiza', 'aléxia', 
        'caroline', 'stephanie', 'helena', 'sabrina', 'maria eduarda', 'raissa', 'nathalia', 'nayara', 'carla', 
        'mary', 'lara', 'cristina', 'sarah', 'michele', 'ana luiza', 'isadora', 'yasmin', 'flávia', 'tainara', 
        'clara', 'laís', 'viviane', 'isabella', 'emanuelle', 'milene', 'lorena', 'heloisa', 'clarice', 'diane', 
        'patrícia', 'monique', 'mayara', 'bianca', 'sofia', 'gabi', 'camilla', 'carol', 'jeniffer', 'izabella', 
        'paola', 'luisa', 'tainara', 'gabriella', 'marcella', 'andreia', 'débora', 'emily', 'renata', 'yumi', 
        'verônica', 'pamela', 'maria luiza', 'francine', 'samara', 'ana carolina', 'babi', 'raiane', 'karen', 
        'ester', 'marcia', 'giovana', 'gabriel', 'lucas', 'matheus', 'pedro', 'leonardo', 'felipe', 'joão', 
        'vinicius', 'guilherme', 'daniel', 'luiz', 'bruno', 'rafael', 'arthur', 'gustavo', 'paulo', 'mateus', 
        'thiago', 'igor', 'douglas', 'victor', 'joao pedro', 'vitor', 'anderson', 'eduardo', 'caio', 'rodrigo', 
        'marcos', 'leandro', 'diego', 'josé', 'fernando', 'fábio', 'joao vitor', 'henrique', 'willian', 'carlos', 
        'marcelo', 'alexandre', 'alex', 'italo', 'raphael', 'flávio', 'bernardo', 'andre', 'luciano', 'ricardo', 
        'luis', 'vagner', 'ramon', 'adriano', 'marcio', 'jeferson', 'david', 'geovanne', 'wesley', 'murilo', 
        'danilo', 'renan', 'augusto', 'maicon', 'sidney', 'pablo', 'breno', 'erick', 'luan', 'emerson', 
        'luiz henrique', 'josue', 'kelvin', 'aline', 'jean', 'samuel', 'hugo', 'davi', 'cristian', 'renato', 
        'jefferson', 'artur', 'geovane', 'raul', 'sávio', 'diogo', 'joão eduardo', 'fabricio', 'juliano', 
        'robert', 'alef', 'jonathan', 'luca', 'patrick', 'junior', 'fred', 'jose matheus', 'tiago', 'giliard', 
        'caue', 'vito', 'kaue', 'zaqueu'    
    ]
    LAST_NAMES = [
        'gonzález', 'rodríguez', 'fernández', 'garcía', 'lópez', 'martínez', 'pérez', 'álvarez', 'gómez', 
        'sánchez', 'díaz', 'vásquez', 'castro', 'romero', 'suárez', 'blanco', 'ruiz', 'alonso', 'torres', 
        'domínguez', 'gutiérrez', 'sosa', 'iglesias', 'giménez', 'ramírez', 'martín', 'varela', 'ramos', 
        'núñez', 'rossi', 'silva', 'méndez', 'hernández', 'flores', 'pereyra', 'ferrari', 'ortiz', 'medina', 
        'benítez', 'herrera', 'arias', 'acosta', 'moreno', 'aguirre', 'otero', 'cabrera', 'rey', 'rojas', 
        'vidal', 'molina', 'russo', 'paz', 'vega', 'costa', 'bruno', 'romano', 'morales', 'ríos', 'miranda', 
        'muñoz', 'franco', 'castillo', 'campos', 'bianchi', 'luna', 'correa', 'ferreyra', 'navarro', 'quiroga', 
        'colombo', 'cohen', 'pereyra', 'vera', 'lorenzo', 'gil', 'santos', 'delgado', 'godoy', 'rivas', 'rivero', 
        'gallo', 'peralta', 'soto', 'figueroa', 'juárez', 'marino', 'ponce', 'calvo', 'ibáñez', 'cáceres', 
        'carrizo', 'vargas', 'mendoza', 'aguilar', 'ledesma', 'guzmán', 'soria', 'villalba', 'prieto', 'maldonado', 
        'silva', 'santos', 'sousa', 'oliveira', 'pereira', 'lima', 'carvalho', 'ferreira', 'rodrigues', 'almeida', 
        'costa', 'gomes', 'martins', 'araújo', 'melo', 'barbosa', 'ribeiro', 'alves', 'cardoso', 'schmitz', 
        'schmidt', 'rocha', 'correia', 'correa', 'dias', 'teixeira', 'fernandes', 'azevedo', 'cavalcante', 
        'cavalcanti', 'montes', 'morais', 'gonçalves'    
    ]
    LOCATION_COUNTRY_IDX    = 0
    LOCATION_STATE_IDX      = 1
    LOCATION_CITY_IDX       = 2
    LOCATION_CUMFREQ_IDX    = 3
    LOCATIONS = [
        (('Brazil',	'RJ', 	'Rio de Janeiro', 	        ), 3094),
        (('Brazil',	'SP', 	'São Paulo', 	            ), 2657),
        (('Brazil',	'SP', 	'Campinas', 	            ), 1347),
        (('Brazil',	'MG', 	'Belo Horizonte', 	        ), 1221),
        (('Brazil',	'RS', 	'Porto Alegre', 	        ), 1131),
        (('Brazil',	'PR', 	'Maringá', 	                ), 1042),
        (('Brazil',	'ES', 	'Vitória', 	                ), 953), 
        (('Brazil',	'GO', 	'Goiânia', 	                ), 944), 
        (('Brazil',	'SP', 	'Sorocaba', 	            ), 941), 
        (('Brazil',	'SC', 	'Florianópolis', 	        ), 936), 
        (('Brazil',	'MS', 	'Campo Grande', 	        ), 932), 
        (('Brazil',	'MG', 	'Juiz de Fora', 	        ), 891), 
        (('Brazil',	'PR', 	'Curitiba', 	            ), 836), 
        (('Brazil',	'SP', 	'Ribeirão Preto', 	        ), 824), 
        (('Brazil',	'SP', 	'Piracicaba', 	            ), 811), 
        (('Brazil',	'AM', 	'Manaus', 	                ), 803), 
        (('Brazil',	'SP', 	'Santos', 	                ), 750), 
        (('Brazil',	'PR', 	'Foz do Iguaçu', 	        ), 723), 
        (('Brazil',	'CE', 	'Fortaleza', 	            ), 721), 
        (('Brazil',	'BA', 	'Salvador', 	            ), 711), 
        (('Brazil',	'MT', 	'Cuiabá', 	                ), 707), 
        (('Brazil',	'SC', 	'Joinville', 	            ), 706), 
        (('Brazil',	'SP', 	'Jundiaí', 	                ), 674), 
        (('Brazil',	'PB', 	'João Pessoa', 	            ), 670), 
        (('Brazil',	'ES', 	'Vila Velha', 	            ), 637), 
        (('Brazil',	'PR', 	'Londrina', 	            ), 634), 
        (('Brazil',	'SP', 	'São José dos Campos', 	    ), 633), 
        (('Brazil',	'SP', 	'Guarulhos', 	            ), 626), 
        (('Brazil',	'PE', 	'Recife', 	                ), 579), 
        (('Brazil',	'RS', 	'Caxias do Sul', 	        ), 576), 
        (('Brazil',	'RN', 	'Natal', 	                ), 571), 
        (('Brazil',	'SP', 	'Guarujá', 	                ), 567), 
        (('Brazil',	'SP', 	'São José do Rio Preto', 	), 556), 
        (('Brazil',	'MA', 	'São Luís', 	            ), 545), 
        (('Brazil',	'MG', 	'Uberaba', 	                ), 528), 
        (('Brazil',	'RJ', 	'Duque de Caxias', 	        ), 522), 
        (('Brazil',	'MG', 	'Uberlândia', 	            ), 521), 
        (('Brazil',	'PI', 	'Teresina', 	            ), 510), 
        (('Brazil',	'SP', 	'Mauá', 	                ), 510), 
        (('Brazil',	'SP', 	'Santo André', 	            ), 508), 
        (('Brazil',	'SC', 	'Itajaí', 	                ), 503), 
        (('Brazil',	'RJ', 	'Petrópolis', 	            ), 480), 
        (('Brazil',	'ES', 	'Serra', 	                ), 479), 
        (('Brazil',	'SP', 	'São Bernardo do Campo', 	), 475), 
        (('Brazil',	'SP', 	'Hortolândia', 	            ), 467), 
        (('Brazil',	'PA', 	'Belém', 	                ), 463), 
        (('Brazil',	'MG', 	'Contagem', 	            ), 440), 
        (('Brazil',	'SE', 	'Aracaju', 	                ), 439), 
        (('Brazil',	'SP', 	'Taubaté', 	                ), 438), 
        (('Brazil',	'RO', 	'Porto Velho', 	            ), 435), 
        (('Brazil',	'TO', 	'Palmas', 	                ), 429), 
        (('Brazil',	'SP', 	'Indaiatuba', 	            ), 426), 
        (('Brazil',	'MG', 	'Betim', 	                ), 417), 
        (('Brazil',	'PR', 	'São José dos Pinhais', 	), 405), 
        (('Brazil',	'SP', 	'Rio Claro', 	            ), 404), 
        (('Brazil',	'RJ', 	'Niterói', 	                ), 399), 
        (('Brazil',	'MG', 	'Governador Valadares', 	), 397), 
        (('Brazil',	'AL', 	'Maceió', 	                ), 396), 
        (('Brazil',	'SP', 	'Diadema', 	                ), 389), 
        (('Brazil',	'PR', 	'Cascavel', 	            ), 388), 
        (('Brazil',	'MG', 	'Poços de Caldas', 	        ), 378), 
        (('Brazil',	'RJ', 	'Volta Redonda', 	        ), 362), 
        (('Brazil',	'PR', 	'Paranaguá', 	            ), 361), 
        (('Brazil',	'SP', 	'Ribeirão Pires', 	        ), 359), 
        (('Brazil',	'RJ', 	'Angra dos Reis', 	        ), 357), 
        (('Brazil',	'SP', 	'Barretos', 	            ), 352), 
        (('Brazil',	'RS', 	'São Leopoldo', 	        ), 346), 
        (('Brazil',	'RJ', 	'Resende', 	                ), 334), 
        (('Brazil',	'RJ', 	'Campos dos Goytacazes', 	), 329), 
        (('Brazil',	'SP', 	'São Caetano do Sul', 	    ), 328), 
        (('Brazil',	'GO', 	'Anápolis (NA)', 	        ), 326), 
        (('Brazil',	'RJ', 	'Barra do Pirai', 	        ), 323), 
        (('Brazil',	'SP', 	'Barueri', 	                ), 322), 
        (('Brazil',	'PE', 	'Jaboatão dos Guararapes', 	), 319), 
        (('Brazil',	'ES', 	'Cariacica', 	            ), 316), 
        (('Brazil',	'SP', 	'Sumaré', 	                ), 315), 
        (('Brazil',	'RS', 	'Canoas', 	                ), 314), 
        (('Brazil',	'SP', 	'Cubatão', 	                ), 314), 
        (('Brazil',	'RJ', 	'Barra Mansa', 	            ), 307), 
        (('Brazil',	'SP', 	'Osasco', 	                ), 303), 
        (('Brazil',	'SP', 	'Mogi das Cruzes', 	        ), 297), 
        (('Brazil',	'SP', 	'Araçatuba', 	            ), 294), 
        (('Brazil',	'SP', 	'Itapecerica da Serra', 	), 293), 
        (('Brazil',	'RJ', 	'Macaé', 	                ), 290), 
        (('Brazil',	'RR', 	'Boa Vista', 	            ), 279), 
        (('Brazil',	'SP', 	'Mogi Mirim', 	            ), 277), 
        (('Brazil',	'RS', 	'Rio Grande', 	            ), 269), 
        (('Brazil',	'SP', 	'Cotia', 	                ), 267), 
        (('Brazil',	'SP', 	'Valinhos', 	            ), 261), 
        (('Brazil',	'RS', 	'Gravataí', 	            ), 258), 
        (('Brazil',	'AC', 	'Rio Branco', 	            ), 255), 
        (('Brazil',	'ES', 	'Guarapari', 	            ), 243), 
        (('Brazil',	'SP', 	'Pindamonhangaba', 	        ), 234), 
        (('Brazil',	'RJ', 	'São Gonçalo', 	            ), 233), 
        (('Brazil',	'PR', 	'Araucária', 	            ), 230), 
        (('Brazil',	'MG', 	'Ouro Preto', 	            ), 229), 
        (('Brazil',	'ES', 	'Linhares', 	            ), 228), 
        (('Brazil',	'SC', 	'Rio do Sul', 	            ), 226), 
        (('Brazil',	'SP', 	'Atibaia (NA)', 	        ), 220), 
        (('Brazil',	'SP', 	'Guaratinguetá', 	        ), 213)
    ]
    LOCATIONS_CUMFREQ = CumFreqTable.new()
    
    @classmethod
    def pick_location(klass):
        if klass.LOCATIONS_CUMFREQ.maxcumfreq == 0:            
            for value, freq in klass.LOCATIONS:
                klass.LOCATIONS_CUMFREQ.add_row(value, freq)
        country, state, city = klass.LOCATIONS_CUMFREQ.pick()
        return country, state, city
        
    @classmethod
    def random(klass, db_state):
        first_name = random.choice(klass.FIRST_NAMES)
        last_name = random.choice(klass.LAST_NAMES) + ' ' + random.choice(klass.LAST_NAMES)
        country, state, city = klass.pick_location()
        return klass(
            id	           = None
        ,   first_name	   = first_name
        ,   last_name	   = last_name
        ,   company	       = klass.DEFAULT_COMPANY
        ,   address	       = klass.DEFAULT_ADDRESS
        ,   city	       = city
        ,   state	       = state
        ,   country	       = country
        ,   postal_code	   = klass.DEFAULT_POSTAL_CODE
        ,   phone	       = klass.DEFAULT_PHONE
        ,   fax	           = None
        ,   email	       = klass.DEFAULT_EMAIL
        ,   support_rep_id = klass.DEFAULT_SUPPORT_REP_ID
        ,   db_state       = db_state
        ,   preferences    = db_state.pick_genre_preference(klass.PREFERENCE_COUNT)
        ,   tracks_bought  = []
        )

@dataclass
class InvoiceLine:
    id          : int
    invoice_id  : int
    track_id    : int
    unit_price  : float
    quantity    : int

    @classmethod
    def new(klass, invoice, track):
        return klass(
            id          = None
        ,   invoice_id  = invoice.id
        ,   track_id    = track.id
        ,   unit_price  = track.unit_price
        ,   quantity    = 1
        )

@dataclass
class Invoice:
    id	         : int
    customer_id	 : int
    invoice_date : str	
    address	     : str
    city	     : str
    state	     : str
    country	     : str
    postal_code	 : str
    total        : float

    @classmethod
    def new(klass, date, customer):
        return klass(
            id	         = None
        ,   customer_id	 = customer.id
        ,   invoice_date = date
        ,   address	     = Customer.DEFAULT_ADDRESS
        ,   city	     = customer.city
        ,   state	     = customer.state
        ,   country	     = customer.country
        ,   postal_code	 = Customer.DEFAULT_POSTAL_CODE
        ,   total        = 0.0
        )
    
class App(object):
    
    GLOBAL_MEAN         = 500.0
    GLOBAL_SIGMA        = 25.0
    DAILY_GROWTH_FACTOR = 0.000548095
    NOISE_MEAN          = 5 
    NOISE_SIGMA         = 2
    

    MONTH_PARAMETERS = {
         1 : { 'mu': GLOBAL_MEAN, 'sigma': GLOBAL_SIGMA, 'seasonality': 0.198999250 }
    ,    2 : { 'mu': GLOBAL_MEAN, 'sigma': GLOBAL_SIGMA, 'seasonality': 0.153029480 }
    ,    3 : { 'mu': GLOBAL_MEAN, 'sigma': GLOBAL_SIGMA, 'seasonality': 0.057384566 }
    ,    4 : { 'mu': GLOBAL_MEAN, 'sigma': GLOBAL_SIGMA, 'seasonality': 0.000000000 }
    ,    5 : { 'mu': GLOBAL_MEAN, 'sigma': GLOBAL_SIGMA, 'seasonality': 0.033634888 }
    ,    6 : { 'mu': GLOBAL_MEAN, 'sigma': GLOBAL_SIGMA, 'seasonality': 0.127365468 }
    ,    7 : { 'mu': GLOBAL_MEAN, 'sigma': GLOBAL_SIGMA, 'seasonality': 0.195016278 }
    ,    8 : { 'mu': GLOBAL_MEAN, 'sigma': GLOBAL_SIGMA, 'seasonality': 0.174389475 }
    ,    9 : { 'mu': GLOBAL_MEAN, 'sigma': GLOBAL_SIGMA, 'seasonality': 0.084449246 }
    ,   10 : { 'mu': GLOBAL_MEAN, 'sigma': GLOBAL_SIGMA, 'seasonality': 0.007886223 }
    ,   11 : { 'mu': GLOBAL_MEAN, 'sigma': GLOBAL_SIGMA, 'seasonality': 0.015092097 }
    ,   12 : { 'mu': GLOBAL_MEAN, 'sigma': GLOBAL_SIGMA, 'seasonality': 0.099441819 }
    }
    

    def __init__(self, in_db, out_db, num_customers, start_date, end_date):
        assert in_db.endswith('.db')
        assert out_db.endswith('.db')
        assert in_db != out_db
        assert os.path.exists(in_db)
        self.in_db          = in_db
        self.out_db         = out_db
        self.num_customers  = num_customers
        self.start_date     = start_date
        self.end_date       = end_date
        self.factor         = 1.0
    
    def info(self, msg):
        when = str(dt.datetime.now())
        print(f"INFO - {when} - {msg}", file=sys.stderr)
    
    def copy_db(self):
        self.info(f'copying db from {self.in_db} to {self.out_db}')
        shutil.copyfile(self.in_db, self.out_db)
        
    def connect_db(self):
        self.info(f"connecting to db at {self.out_db}")
        db = Db(self.out_db)
        return db
    
    def fetch_state(self, db):
        self.info('fetching application state')
        state = State.new()
        db.open()
        entries = db.fetch_music_data()
        db.close()
        for entry in entries:
            state.process_entry(entry)
        state.fill_genre_cumfreqs()
        #state.show()
        return state
    
    def create_customers(self, db, state):
        self.info(f'creating {self.num_customers} customers')
        db.open()
        for i in range(self.num_customers):
            c = state.create_customer(db)
            #self.info(f'saving {c}')
        db.commit()
        db.close()

    def compute_num_invoices(self, date):
        parameters       = self.MONTH_PARAMETERS[ date.month ]
        effective_factor = self.factor + parameters['seasonality']
        noise            = random.normalvariate(self.NOISE_MEAN, self.NOISE_SIGMA)
        r                = random.normalvariate(parameters['mu'], parameters['sigma'])
        result           = int((r + noise) * effective_factor)
        self.factor     += self.DAILY_GROWTH_FACTOR
        return result
        
    def create_invoices(self, db, state):
        self.info(f'creating invoices')
        db.open()
        date = self.start_date
        while date < self.end_date:
            created_invoices = 0
            num_invoices = self.compute_num_invoices(date)
            #self.info(f'creating {num_invoices} invoices for date {date}')
            for i in range(num_invoices):
                created_invoices += state.create_invoice(db, date)
            self.info(f'created {created_invoices} from {num_invoices} invoices computed for date {date}')
            db.commit() # intermediate commit
            date = date + dt.timedelta(1)
        db.close()
        
    def run(self):
        self.info('starting invoice generator')
        self.copy_db()
        db = self.connect_db()
        state = self.fetch_state(db)
        self.create_customers(db, state)
        self.create_invoices(db, state)
        self.info('finished')
        
if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    date_type = dt.date.fromisoformat
    parser.add_argument('in_db',         type=str,       help='input OLTP DB')
    parser.add_argument('out_db',        type=str,       help='output OLTP DB')
    parser.add_argument('num_customers', type=int,       help='number of customers')
    parser.add_argument('start_date',    type=date_type, help='start date')
    parser.add_argument('end_date',      type=date_type, help='end date')
    args = parser.parse_args()
    app = App(
        args.in_db
    ,   args.out_db
    ,   args.num_customers
    ,   args.start_date
    ,   args.end_date
    )
    app.run()
    
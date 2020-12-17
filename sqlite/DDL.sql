DROP TABLE film;
DROP TABLE inventory;

CREATE TABLE film (
film_id int,
PRIMARY KEY (film_id)
);

CREATE TABLE inventory (
inventory_id int,
film_id int,
PRIMARY KEY (inventory_id)
CONSTRAINT fk_film_id FOREIGN KEY(film_id) REFERENCES film(film_id)
);

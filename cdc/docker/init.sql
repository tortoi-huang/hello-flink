CREATE TABLE IF NOT EXISTS person (
    id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    person_no VARCHAR(32),
    first_name VARCHAR(32),
    update_date DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    create_date DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
insert into person(person_no, first_name) values('1', 'hello'),('2', 'world'),('3', '.');

update person set first_name='...' where person_no='3';
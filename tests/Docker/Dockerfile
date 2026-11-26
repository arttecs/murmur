FROM php:8.4-cli-alpine

RUN docker-php-ext-install pcntl

COPY --from=composer:2 /usr/bin/composer /usr/bin/composer

WORKDIR /murmur

COPY composer.json ./
RUN composer update --no-interaction --no-progress --prefer-dist

COPY . .
RUN composer dump-autoload --no-interaction

CMD ["php", "vendor/bin/testbench", "murmur:serve"]

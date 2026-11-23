<?php

use Arttecs\Murmur\Server\Auth\Signature;

it('verifies a publish signature within tolerance', function () {
    $sig = Signature::sign('s3cret-s3cret-s3cret-s3cret-s3cret', 1000, '{"a":1}');
    expect(Signature::verify('s3cret-s3cret-s3cret-s3cret-s3cret', 1000, '{"a":1}', $sig, 1059))->toBeTrue();
    expect(Signature::verify('s3cret-s3cret-s3cret-s3cret-s3cret', 1000, '{"a":1}', $sig, 1061))->toBeFalse();
    expect(Signature::verify('s3cret-s3cret-s3cret-s3cret-s3cret', 1000, '{"a":2}', $sig, 1000))->toBeFalse();
    expect(Signature::verify('other-other-other-other-other-other', 1000, '{"a":1}', $sig, 1000))->toBeFalse();
});

<?php

namespace App;

use Tempest\Console\ConsoleCommand;
use Tempest\Console\HasConsole;

final class DataParseCommand
{
    use HasConsole;

    #[ConsoleCommand]
    public function __invoke(): void
    {


        $this->success('Done');
    }
}
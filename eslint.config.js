import html from "eslint-plugin-html";

export default [
    {
        files: ["**/*.html"],
        plugins: { html },
    },
    {
        files: ["**/*.html", "**/*.js"],
        languageOptions: {
            ecmaVersion: 2022,
            sourceType: "script",
            globals: {
                // Browser globals
                window: "readonly",
                document: "readonly",
                console: "readonly",
                fetch: "readonly",
                history: "readonly",
                location: "readonly",
                navigator: "readonly",
                HTMLElement: "readonly",
                Event: "readonly",
                URLSearchParams: "readonly",
                Promise: "readonly",
                Set: "readonly",
                Map: "readonly",
                Option: "readonly",
                setTimeout: "readonly",
                clearTimeout: "readonly",

                // CDN libraries loaded via <script> tags
                Tabulator: "readonly",
                Papa: "readonly",
                L: "readonly",
                Chart: "readonly",
            },
        },
        rules: {
            "no-undef": "error",
            "no-use-before-define": ["error", { functions: false, classes: false }],
            "no-unused-vars": ["warn", { argsIgnorePattern: "^_" }],
            "no-redeclare": "error",
        },
    },
    {
        ignores: ["node_modules/", "data/"],
    },
];

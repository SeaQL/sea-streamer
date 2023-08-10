import { hello } from "./index";

test('hello', () => {
    expect(hello()).toStrictEqual("world");
});
using System;
using System.Text;
using System.Text.Json;

namespace Dash.Internal;

/// <summary>
/// A <see cref="JsonNamingPolicy"/> that converts PascalCase
/// property names to <c>snake_case</c>.
///
/// We hand-roll this because <see cref="JsonNamingPolicy.SnakeCaseLower"/>
/// is .NET 8+ only and we also target <c>netstandard2.0</c>. The
/// implementation follows the same rules as the BCL version:
/// uppercase runs become lower-case and are joined with single
/// underscores.
/// </summary>
internal sealed class SnakeCaseLowerNamingPolicy : JsonNamingPolicy
{
    public static readonly SnakeCaseLowerNamingPolicy Instance = new();

    public override string ConvertName(string name)
    {
        if (string.IsNullOrEmpty(name))
        {
            return name;
        }

        var builder = new StringBuilder(name.Length + 8);
        for (var i = 0; i < name.Length; i++)
        {
            var c = name[i];

            // An underscore in the source name is treated as a word
            // boundary so PascalCase like "Foo_Bar" still works.
            if (c == '_')
            {
                builder.Append('_');
                continue;
            }

            var isUpper = char.IsUpper(c);
            if (isUpper)
            {
                if (i > 0)
                {
                    var prev = name[i - 1];
                    var next = i + 1 < name.Length ? name[i + 1] : '\0';

                    // Insert an underscore when transitioning from
                    // lower to upper (fooBar -> foo_bar) or between
                    // two uppercase letters followed by a lower
                    // (FOOBar -> foo_bar).
                    var prevIsLower = char.IsLower(prev) || char.IsDigit(prev);
                    var nextIsLower = char.IsLower(next);
                    var prevIsUpper = char.IsUpper(prev);
                    if (prevIsLower || (prevIsUpper && nextIsLower && prev != '_'))
                    {
                        builder.Append('_');
                    }
                }

                builder.Append(char.ToLowerInvariant(c));
            }
            else
            {
                builder.Append(c);
            }
        }

        return builder.ToString();
    }
}

/*
 * AkkaraDB - The all-purpose KV store: blazing fast and reliably durable, scaling from tiny embedded cache to large-scale distributed database
 * Copyright (C) 2026 Swift Storm Studio
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */

// akkaradb/include/akkaradb/detail/PTUpdateHooks.hpp
#pragma once

void runUpdateFieldHooks(const Entity& oldEntity, Entity& newEntity) {
    for (const auto& hook : updateFieldHooks_) {
        if (hook.unchanged(oldEntity, newEntity)) { continue; }
        hook.callback(oldEntity, newEntity);
    }
}

template <auto FieldPtr, typename Handler>
static void invokeFieldUpdateHandler(Handler& handler, const Entity& oldEntity, Entity& newEntity) {
    using Field = binpack::detail::memberOf<FieldPtr>;
    if constexpr (std::invocable<Handler&, const Field&, Field&, const Entity&, Entity&>) {
        handler(oldEntity.*FieldPtr, newEntity.*FieldPtr, oldEntity, newEntity);
    }
    else if constexpr (std::invocable<Handler&, const Field&, Field&>) { handler(oldEntity.*FieldPtr, newEntity.*FieldPtr); }
    else if constexpr (std::invocable<Handler&, const Entity&, Entity&>) { handler(oldEntity, newEntity); }
    else if constexpr (std::invocable<Handler&, const decltype(oldEntity.*FieldPtr)&, const decltype(newEntity.*FieldPtr)&,
        const Entity&, const Entity&>) { handler(oldEntity.*FieldPtr, newEntity.*FieldPtr, oldEntity, newEntity); }
    else if constexpr (std::invocable<Handler&, const decltype(oldEntity.*FieldPtr)&, const decltype(newEntity.*FieldPtr)&>) {
        handler(oldEntity.*FieldPtr, newEntity.*FieldPtr);
    }
    else if constexpr (std::invocable<Handler&, const Entity&, const Entity&>) { handler(oldEntity, newEntity); }
    else {
        static_assert(
            !sizeof(Handler),
            "onUpdate handler must accept one of: " "(oldField, Field& newField), "
            "(oldField, Field& newField, oldEntity, Entity& newEntity), " "(oldEntity, Entity& newEntity), "
            "(oldField, newField), " "(oldField, newField, oldEntity, newEntity), " "or (oldEntity, newEntity)"
        );
    }
}
